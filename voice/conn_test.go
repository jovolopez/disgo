package voice

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/disgoorg/godave"
	"github.com/disgoorg/snowflake/v2"
	"github.com/gorilla/websocket"

	"github.com/disgoorg/disgo/discord"
	botgateway "github.com/disgoorg/disgo/gateway"
)

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

func TestChannelMoveRebuildsTransportsAndReopensGateway(t *testing.T) {
	var gateways []*gatewayStub
	var udps []*udpConnStub
	conn := NewConn(1, 2,
		func(context.Context, snowflake.ID, *snowflake.ID, bool, bool) error { return nil },
		func() {},
		WithConnGatewayCreateFunc(func(godave.Session, EventHandlerFunc, CloseHandlerFunc, ...GatewayConfigOpt) Gateway {
			g := &gatewayStub{opened: make(chan State, 4)}
			gateways = append(gateways, g)
			return g
		}),
		WithUDPConnCreateFunc(func(godave.Session, SsrcLookupFunc, ...UDPConnConfigOpt) UDPConn {
			u := &udpConnStub{}
			udps = append(udps, u)
			return u
		}),
	).(*connImpl)

	channelID := snowflake.ID(3)
	conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
		GuildID:   1,
		UserID:    2,
		ChannelID: &channelID,
		SessionID: "sess-1",
	}})
	endpoint1 := "end1.discord.media"
	conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{
		GuildID:  1,
		Token:    "tok-1",
		Endpoint: &endpoint1,
	})

	select {
	case <-gateways[0].opened:
	case <-time.After(time.Second):
		t.Fatal("gateway did not open for initial join")
	}

	conn.ssrcsMu.Lock()
	conn.ssrcs[42] = 99
	conn.ssrcsMu.Unlock()

	// moving to another channel must rebuild the transports
	channelID2 := snowflake.ID(4)
	conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
		GuildID:   1,
		UserID:    2,
		ChannelID: &channelID2,
		SessionID: "sess-1",
	}})

	if len(gateways) != 2 || len(udps) != 2 {
		t.Fatalf("expected transports to be rebuilt on move, gateways=%d udp conns=%d", len(gateways), len(udps))
	}
	if gateways[0].closeCalls == 0 {
		t.Error("expected old gateway to be closed on move")
	}
	if udps[0].closeCalls != 1 {
		t.Errorf("expected old udp conn to be closed once on move, got %d", udps[0].closeCalls)
	}
	if udps[1].closeCalls != 0 {
		t.Errorf("expected new udp conn to stay open, got %d close calls", udps[1].closeCalls)
	}
	conn.ssrcsMu.Lock()
	ssrcsLeft := len(conn.ssrcs)
	conn.ssrcsMu.Unlock()
	if ssrcsLeft != 0 {
		t.Errorf("expected stale ssrcs to be cleared on move, got %d", ssrcsLeft)
	}

	// the new gateway must wait for a fresh voice server update
	select {
	case state := <-gateways[1].opened:
		t.Fatalf("gateway reopened before fresh voice server update: %+v", state)
	case <-time.After(20 * time.Millisecond):
	}

	endpoint2 := "end2.discord.media"
	conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{
		GuildID:  1,
		Token:    "tok-2",
		Endpoint: &endpoint2,
	})

	select {
	case state := <-gateways[1].opened:
		if state.ChannelID != 4 || state.SessionID != "sess-1" || state.Token != "tok-2" || state.Endpoint != endpoint2 {
			t.Errorf("reopened gateway with wrong state: %+v", state)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not reopen after move")
	}
	select {
	case state := <-gateways[0].opened:
		t.Errorf("stale gateway generation was reopened: %+v", state)
	default:
	}
}

func TestStaleGenerationCallbacksAreIgnored(t *testing.T) {
	staleGateway := &gatewayStub{opened: make(chan State, 1)}
	currentGateway := &gatewayStub{opened: make(chan State, 1)}
	currentUDP := &udpConnStub{}
	var updates []*snowflake.ID
	var removed, openedCalled bool
	conn := &connImpl{
		config: defaultConnConfig(),
		state: State{
			GuildID:   1,
			UserID:    2,
			ChannelID: 3,
			SessionID: "sess",
			Token:     "tok",
			Endpoint:  "end.discord.media",
		},
		gateway: currentGateway,
		udp:     currentUDP,
		voiceStateUpdateFunc: func(_ context.Context, _ snowflake.ID, channelID *snowflake.ID, _, _ bool) error {
			updates = append(updates, channelID)
			return nil
		},
		removeConnFunc: func() { removed = true },
		openedFunc:     func() { openedCalled = true },
	}

	// a close from a replaced generation must not tear down or reopen anything
	conn.handleGatewayClose(staleGateway, errors.New("voice endpoint is unavailable"))
	if len(updates) != 0 || removed {
		t.Errorf("stale close triggered reconnect/teardown: updates=%v removed=%v", updates, removed)
	}
	if currentUDP.closeCalls != 0 {
		t.Errorf("stale close closed the current udp conn: %d", currentUDP.closeCalls)
	}
	if currentGateway.closeCalls != 0 {
		t.Errorf("stale close closed the current gateway: %d", currentGateway.closeCalls)
	}

	// a session description from a replaced generation must not touch current transports
	conn.speakingFlags = SpeakingFlagMicrophone
	conn.speakingSet = true
	conn.handleMessage(staleGateway, OpcodeSessionDescription, 0, GatewayMessageDataSessionDescription{
		Mode:      EncryptionModeAEADXChaCha20Poly1305RTPSize,
		SecretKey: make([]byte, 32),
	})
	if currentUDP.setSecretKeyCalls != 0 {
		t.Errorf("stale session description configured the current udp conn: %d", currentUDP.setSecretKeyCalls)
	}
	if openedCalled {
		t.Error("stale session description fired the opened callback")
	}
	if len(currentGateway.sends) != 0 {
		t.Errorf("stale session description sent messages on the current gateway: %d", len(currentGateway.sends))
	}

	// the current generation must still be processed, with speaking restored
	conn.handleMessage(currentGateway, OpcodeSessionDescription, 0, GatewayMessageDataSessionDescription{
		Mode:      EncryptionModeAEADXChaCha20Poly1305RTPSize,
		SecretKey: make([]byte, 32),
	})
	if currentUDP.setSecretKeyCalls != 1 {
		t.Errorf("expected current udp conn to get the secret key once, got %d", currentUDP.setSecretKeyCalls)
	}
	if !openedCalled {
		t.Error("expected opened callback to fire for current generation")
	}
	if len(currentGateway.sends) != 1 {
		t.Fatalf("expected speaking to be restored once, got %d sends", len(currentGateway.sends))
	}
	send := currentGateway.sends[0]
	speaking, ok := send.data.(GatewayMessageDataSpeaking)
	if send.op != OpcodeSpeaking || !ok || speaking.Speaking != SpeakingFlagMicrophone {
		t.Errorf("expected speaking flag restore, got op=%v data=%+v", send.op, send.data)
	}
}

func TestHandleGatewayCloseWaitsForFreshUpdatesOn4014And4022(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{name: "disconnected", code: GatewayCloseEventCodeDisconnected.Code},
		{name: "call terminated", code: GatewayCloseEventCodeCallTerminated.Code},
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
					SessionID: "sess",
					Token:     "tok",
					Endpoint:  "end.discord.media",
				},
				gateway: gateway,
				udp:     udp,
				voiceStateUpdateFunc: func(_ context.Context, _ snowflake.ID, channelID *snowflake.ID, _, _ bool) error {
					updates = append(updates, channelID)
					return nil
				},
				removeConnFunc: func() { removed = true },
			}

			conn.handleGatewayClose(gateway, &websocket.CloseError{Code: tt.code})

			if len(updates) != 0 {
				t.Errorf("expected to wait for fresh updates instead of reopening, got %v", updates)
			}
			if removed {
				t.Error("expected conn to stay registered while waiting for fresh updates")
			}
			if udp.closeCalls != 0 {
				t.Errorf("expected udp conn to stay open, got %d close calls", udp.closeCalls)
			}
			if conn.state.SessionID != "sess" || conn.state.Token != "tok" || conn.state.Endpoint != "end.discord.media" {
				t.Errorf("expected voice credentials to stay intact, got %+v", conn.state)
			}
		})
	}
}

type gatewayStub struct {
	closeCalls int
	opened     chan State
	sends      []stubSend
}

type stubSend struct {
	op   Opcode
	data GatewayMessageData
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
	g.sends = append(g.sends, stubSend{op: op, data: data})
	return nil
}

func (*gatewayStub) Latency() time.Duration { return 0 }

type udpConnStub struct {
	closeCalls        int
	setSecretKeyCalls int
}

func (*udpConnStub) LocalAddr() net.Addr { return nil }

func (*udpConnStub) RemoteAddr() net.Addr { return nil }

func (u *udpConnStub) SetSecretKey(EncryptionMode, []byte) error {
	u.setSecretKeyCalls++
	return nil
}

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
