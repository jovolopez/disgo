package voice

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/disgoorg/godave"
	"github.com/disgoorg/snowflake/v2"
	"github.com/gorilla/websocket"

	"github.com/disgoorg/disgo/discord"
	botgateway "github.com/disgoorg/disgo/gateway"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestHandleGatewayCloseStartsFreshConnection(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "invalid session",
			err:  &websocket.CloseError{Code: GatewayCloseEventCodeSessionNoLongerValid.Code},
		},
		{
			name: "resume attempts exhausted",
			err:  errors.New("voice endpoint is unavailable"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway := &gatewayStub{}
			udp := &udpConnStub{}
			var updates []*snowflake.ID
			var stateAtUpdate State
			var removed bool
			var conn *connImpl

			conn = &connImpl{
				config: defaultConnConfig(),
				state: State{
					GuildID:   1,
					UserID:    2,
					ChannelID: 3,
					SessionID: "old-session",
					Token:     "old-token",
					Endpoint:  "old.discord.media",
				},
				gateway: gateway,
				udp:     udp,
				voiceStateUpdateFunc: func(_ context.Context, _ snowflake.ID, channelID *snowflake.ID, _ bool, _ bool) error {
					updates = append(updates, channelID)
					if channelID != nil {
						stateAtUpdate = conn.state
						conn.openedFunc()
					}
					return nil
				},
				removeConnFunc: func() {
					removed = true
				},
			}

			conn.handleGatewayClose(gateway, tt.err)

			if len(updates) != 1 || updates[0] == nil || *updates[0] != 3 {
				t.Fatalf("expected one voice state update to channel 3, got %v", updates)
			}
			if stateAtUpdate.SessionID != "" || stateAtUpdate.Token != "" || stateAtUpdate.Endpoint != "" {
				t.Fatalf("expected cached voice credentials to be cleared, got %+v", stateAtUpdate)
			}
			if gateway.closeCalls != 0 {
				t.Errorf("expected reopened gateway to remain open, got %d close calls", gateway.closeCalls)
			}
			if udp.closeCalls != 1 {
				t.Errorf("expected stale UDP connection to be closed once, got %d close calls", udp.closeCalls)
			}
			if removed {
				t.Error("expected connection to remain registered")
			}
		})
	}
}

func TestHandleGatewayCloseIgnoresMoveCloseCodes(t *testing.T) {
	tests := []struct {
		name         string
		code         int
		wantTeardown bool
	}{
		{
			name: "disconnected",
			code: GatewayCloseEventCodeDisconnected.Code,
		},
		{
			name: "call terminated",
			code: GatewayCloseEventCodeCallTerminated.Code,
		},
		{
			name:         "rate limited",
			code:         GatewayCloseEventCodeRateLimited.Code,
			wantTeardown: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway := &gatewayStub{}
			udp := &udpConnStub{}
			var updates []*snowflake.ID
			var removed bool
			conn := &connImpl{
				config: defaultConnConfig(),
				state: State{
					GuildID:   1,
					UserID:    2,
					ChannelID: 3,
					SessionID: "old-session",
					Token:     "old-token",
					Endpoint:  "old.discord.media",
				},
				gateway: gateway,
				udp:     udp,
				dave:    godave.NewNoopSession(discardLogger, godave.UserID("2"), nil),
				voiceStateUpdateFunc: func(_ context.Context, _ snowflake.ID, channelID *snowflake.ID, _ bool, _ bool) error {
					updates = append(updates, channelID)
					return nil
				},
				removeConnFunc: func() {
					removed = true
				},
			}

			conn.handleGatewayClose(gateway, &websocket.CloseError{Code: tt.code})

			if tt.wantTeardown {
				if !removed {
					t.Error("expected the conn to be removed")
				}
				if len(updates) != 1 || updates[0] != nil {
					t.Errorf("expected one leave update, got %v", updates)
				}
				return
			}
			// 4014 & 4022 also mark a channel move: the conn must survive and
			// reconnect once the fresh voice events arrive.
			if removed {
				t.Error("expected the conn to stay registered")
			}
			if len(updates) != 0 {
				t.Errorf("expected no voice state updates, got %v", updates)
			}
			if gateway.closeCalls != 0 || udp.closeCalls != 0 {
				t.Errorf("expected transports to stay untouched, got %d gateway and %d udp close calls", gateway.closeCalls, udp.closeCalls)
			}
		})
	}
}

func TestVoiceGatewayWaitsForFreshStateAndServerUpdates(t *testing.T) {
	opened := make(chan State, 1)
	conn := &connImpl{
		config: defaultConnConfig(),
		state: State{
			GuildID:   1,
			UserID:    2,
			ChannelID: 3,
		},
		gateway: &gatewayStub{opened: opened},
	}
	endpoint := "new.discord.media"

	conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{
		GuildID:  1,
		Token:    "new-token",
		Endpoint: &endpoint,
	})

	select {
	case state := <-opened:
		t.Fatalf("gateway opened before fresh voice state update: %+v", state)
	case <-time.After(20 * time.Millisecond):
	}

	channelID := snowflake.ID(3)
	conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
		GuildID:   1,
		UserID:    2,
		ChannelID: &channelID,
		SessionID: "new-session",
	}})

	select {
	case state := <-opened:
		if state.SessionID != "new-session" || state.Token != "new-token" || state.Endpoint != endpoint {
			t.Fatalf("gateway opened with incomplete fresh state: %+v", state)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not open after both fresh updates arrived")
	}
}

func TestChannelMoveRebuildsTransports(t *testing.T) {
	var gateways []*gatewayStub
	var udps []*udpConnStub
	var updates int
	var removed bool
	var conn *connImpl

	conn = NewConn(1, 2,
		func(_ context.Context, _ snowflake.ID, channelID *snowflake.ID, _ bool, _ bool) error {
			if channelID != nil {
				updates++
			}
			if conn.openedFunc != nil {
				conn.openedFunc()
			}
			return nil
		},
		func() {
			removed = true
		},
		WithConnDaveSessionLogger(discardLogger),
		WithConnGatewayCreateFunc(func(_ godave.Session, _ EventHandlerFunc, _ CloseHandlerFunc, _ ...GatewayConfigOpt) Gateway {
			g := &gatewayStub{opened: make(chan State, 1)}
			gateways = append(gateways, g)
			return g
		}),
		WithUDPConnCreateFunc(func(_ godave.Session, _ SsrcLookupFunc, _ ...UDPConnConfigOpt) UDPConn {
			u := &udpConnStub{}
			udps = append(udps, u)
			return u
		}),
	).(*connImpl)

	// join channel 3
	channelID := snowflake.ID(3)
	conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
		GuildID:   1,
		UserID:    2,
		ChannelID: &channelID,
		SessionID: "session-1",
	}})
	endpoint1 := "a.discord.media"
	conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{
		GuildID:  1,
		Token:    "token-1",
		Endpoint: &endpoint1,
	})
	waitGatewayOpened(t, gateways[0].opened, State{ChannelID: 3, SessionID: "session-1", Token: "token-1", Endpoint: endpoint1})

	if err := conn.SetSpeaking(context.Background(), SpeakingFlagMicrophone); err != nil {
		t.Fatalf("failed to set speaking: %v", err)
	}

	// move to channel 4: the old generation must be retired and a fresh one opened
	channelID = snowflake.ID(4)
	conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
		GuildID:   1,
		UserID:    2,
		ChannelID: &channelID,
		SessionID: "session-2",
	}})
	if len(gateways) != 2 || len(udps) != 2 {
		t.Fatalf("expected a fresh transport generation after the move, got %d gateways and %d udp conns", len(gateways), len(udps))
	}
	if gateways[0].closeCalls != 1 || udps[0].closeCalls != 1 {
		t.Errorf("expected the old transports to be closed once, got %d gateway and %d udp close calls", gateways[0].closeCalls, udps[0].closeCalls)
	}

	endpoint2 := "b.discord.media"
	conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{
		GuildID:  1,
		Token:    "token-2",
		Endpoint: &endpoint2,
	})
	waitGatewayOpened(t, gateways[1].opened, State{ChannelID: 4, SessionID: "session-2", Token: "token-2", Endpoint: endpoint2})

	// stale callbacks from the retired generation must be ignored
	conn.handleMessage(gateways[0], OpcodeSpeaking, 0, GatewayMessageDataSpeaking{SSRC: 42, UserID: 9})
	if conn.UserIDBySSRC(42) != 0 {
		t.Error("expected speaking from the retired gateway to be ignored")
	}
	conn.handleGatewayClose(gateways[0], errors.New("stale close"))
	if updates != 0 || removed || gateways[1].closeCalls != 0 || udps[1].closeCalls != 0 {
		t.Errorf("expected the stale close to be ignored, got %d reopen updates, removed %v, %d gateway and %d udp close calls", updates, removed, gateways[1].closeCalls, udps[1].closeCalls)
	}

	// the speaking state is restored once the fresh session is established
	conn.handleMessage(gateways[1], OpcodeSessionDescription, 0, GatewayMessageDataSessionDescription{
		Mode:      EncryptionModeAEADAES256GCMRTPSize,
		SecretKey: []byte("secret"),
	})
	if !gateways[1].sentSpeaking(SpeakingFlagMicrophone) {
		t.Error("expected the speaking state to be re-sent on the fresh session")
	}
}

func waitGatewayOpened(t *testing.T, opened <-chan State, want State) {
	t.Helper()
	select {
	case state := <-opened:
		if state.ChannelID != want.ChannelID || state.SessionID != want.SessionID || state.Token != want.Token || state.Endpoint != want.Endpoint {
			t.Fatalf("gateway opened with %+v, want %+v", state, want)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not open")
	}
}

type sentMessage struct {
	op   Opcode
	data GatewayMessageData
}

type gatewayStub struct {
	closeCalls int
	opened     chan State
	sent       []sentMessage
}

func (*gatewayStub) SSRC() uint32 { return 0 }

func (g *gatewayStub) Open(_ context.Context, state State) error {
	if g.opened != nil {
		g.opened <- state
	}
	return nil
}

func (g *gatewayStub) Close() { g.closeCalls++ }

func (*gatewayStub) CloseWithCode(int, string) {}

func (*gatewayStub) Status() Status { return StatusDisconnected }

func (g *gatewayStub) Send(_ context.Context, op Opcode, data GatewayMessageData) error {
	g.sent = append(g.sent, sentMessage{op: op, data: data})
	return nil
}

func (*gatewayStub) Latency() time.Duration { return 0 }

func (g *gatewayStub) sentSpeaking(flags SpeakingFlags) bool {
	for _, msg := range g.sent {
		if msg.op != OpcodeSpeaking {
			continue
		}
		if speaking, ok := msg.data.(GatewayMessageDataSpeaking); ok && speaking.Speaking == flags {
			return true
		}
	}
	return false
}

type udpConnStub struct {
	closeCalls int
}

func (*udpConnStub) LocalAddr() net.Addr { return nil }

func (*udpConnStub) RemoteAddr() net.Addr { return nil }

func (*udpConnStub) SetSecretKey(EncryptionMode, []byte) error { return nil }

func (*udpConnStub) SetDeadline(time.Time) error { return nil }

func (*udpConnStub) SetReadDeadline(time.Time) error { return nil }

func (*udpConnStub) SetWriteDeadline(time.Time) error { return nil }

func (*udpConnStub) Open(context.Context, string, int, uint32) (string, int, error) {
	return "", 0, nil
}

func (u *udpConnStub) Close() error {
	u.closeCalls++
	return nil
}

func (*udpConnStub) Read([]byte) (int, error) { return 0, nil }

func (*udpConnStub) ReadPacket() (*Packet, error) { return nil, nil }

func (*udpConnStub) Write([]byte) (int, error) { return 0, nil }
