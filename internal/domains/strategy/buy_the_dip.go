package strategy

import (
	"fmt"

	"whatever/internal/idgen"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

const BuyTheDipID = "buy_the_dip"

func init() {
	reg.Strategies.Register(BuyTheDipID, func(opts map[string]any) (proto.Strategy, error) {
		id := idgen.GenerateID(BuyTheDipID)

		cfg := DefaultBuyTheDipConfig()

		return &BuyTheDip{
			id:     id,
			cfg:    cfg,
			states: make(map[string]*symbolState),
		}, nil
	})
}

type TrendMaturity int

const (
	NoTrend        TrendMaturity = iota
	EarlyTrend                   // n consecutive higher closes
	ConfirmedTrend               // higher-highs AND higher-lows established
	MatureTrend                  // sustained confirmed trend
)

func (t TrendMaturity) String() string {
	switch t {
	case EarlyTrend:
		return "early"
	case ConfirmedTrend:
		return "confirmed"
	case MatureTrend:
		return "mature"
	default:
		return "none"
	}
}

type BuyTheDipConfig struct {
	EarlyTrendCloses int     // consecutive higher closes for early trend (e.g., 3)
	ConfirmedSwings  int     // HH+HL pairs needed for confirmed (e.g., 2)
	MatureSwings     int     // HH+HL pairs for mature (e.g., 4)
	PullbackPercent  float64 // e.g., 0.03 for 3%
	SwingLookback    int     // candles on each side to identify swing points
	MaxCandles       int     // max candles to retain in rolling window
}

func DefaultBuyTheDipConfig() BuyTheDipConfig {
	return BuyTheDipConfig{
		EarlyTrendCloses: 3,
		ConfirmedSwings:  2,
		MatureSwings:     3,
		PullbackPercent:  0.03,
		SwingLookback:    2,
		MaxCandles:       200,
	}
}

// swingPoint represents a detected swing high or low
type swingPoint struct {
	price  float64
	ts     int64 // candle close timestamp
	isHigh bool
}

// symbolState tracks trend and pullback state for a single symbol
type symbolState struct {
	candles       []proto.Candle
	swingHighs    []swingPoint // ordered by time, oldest first
	swingLows     []swingPoint
	recentHigh    float64
	consecutiveUp int  // consecutive higher closes
	inPullback    bool // price has dropped from recent high
}

type BuyTheDip struct {
	id  string
	cfg BuyTheDipConfig

	data    <-chan proto.MarketData
	signals chan proto.Signal
	errs    chan error
	done    chan struct{}

	states map[string]*symbolState // keyed by symbol
}

func (s *BuyTheDip) ID() string {
	return s.id
}

func (s *BuyTheDip) Init(data <-chan proto.MarketData) error {
	if data == nil {
		return fmt.Errorf("data channel is required")
	}
	s.data = data
	s.signals = make(chan proto.Signal, 100)
	s.errs = make(chan error, 10)
	s.done = make(chan struct{})
	return nil
}

func (s *BuyTheDip) Streams() (<-chan proto.Signal, <-chan error) {
	return s.signals, s.errs
}

func (s *BuyTheDip) Start() error {
	go func() {
		defer close(s.signals)
		defer close(s.errs)

		for {
			select {
			case <-s.done:
				return
			case md, ok := <-s.data:
				if !ok {
					return
				}
				s.processCandle(md)
			}
		}
	}()
	return nil
}

func (s *BuyTheDip) Close() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
}

func (s *BuyTheDip) processCandle(md proto.MarketData) {
	state := s.getOrCreateState(md.Symbol)
	candle := md.Candle

	// append candle and trim to max
	state.candles = append(state.candles, candle)
	if len(state.candles) > s.cfg.MaxCandles {
		state.candles = state.candles[1:]
	}

	// update consecutive higher closes
	if len(state.candles) >= 2 {
		prev := state.candles[len(state.candles)-2]
		if candle.Close > prev.Close {
			state.consecutiveUp++
		} else {
			state.consecutiveUp = 0
		}
	}

	// detect new swing points (need enough candles for lookback)
	s.detectSwingPoints(state)

	// update recent high
	if candle.High > state.recentHigh {
		state.recentHigh = candle.High
		state.inPullback = false
	}

	// check for pullback
	if state.recentHigh > 0 {
		pullbackDepth := (state.recentHigh - candle.Close) / state.recentHigh
		if pullbackDepth >= s.cfg.PullbackPercent {
			state.inPullback = true
		}
	}

	// evaluate trend and emit signal if conditions met
	maturity := s.evaluateTrend(state)
	if maturity != NoTrend && state.inPullback {
		pullbackDepth := (state.recentHigh - candle.Close) / state.recentHigh

		signal := proto.Signal{
			Symbol:    md.Symbol,
			Side:      "BUY",
			Timestamp: candle.CloseTs,
			Source:    s.id,
			Metadata: map[string]any{
				"trend_maturity":   maturity,
				"pullback_depth":   pullbackDepth,
				"recent_high":      state.recentHigh,
				"swing_high_count": len(state.swingHighs),
				"swing_low_count":  len(state.swingLows),
			},
		}

		select {
		case s.signals <- signal:
		case <-s.done:
			return
		}

		// reset pullback state after signal
		state.inPullback = false
	}
}

func (s *BuyTheDip) getOrCreateState(symbol string) *symbolState {
	state, ok := s.states[symbol]
	if !ok {
		state = &symbolState{
			candles:    make([]proto.Candle, 0, s.cfg.MaxCandles),
			swingHighs: make([]swingPoint, 0),
			swingLows:  make([]swingPoint, 0),
		}
		s.states[symbol] = state
	}
	return state
}

// detectSwingPoints identifies swing highs and lows using lookback window.
// A swing high: candle high > highs of n candles on each side.
// A swing low: candle low < lows of n candles on each side.
func (s *BuyTheDip) detectSwingPoints(state *symbolState) {
	n := len(state.candles)
	lb := s.cfg.SwingLookback
	maxSwings := s.cfg.MatureSwings * 2

	// need at least 2*lookback + 1 candles, and we check the middle candle
	// (lookback candles ago, so we have lookback candles after it)
	if n < 2*lb+1 {
		return
	}

	// check the candle at position n - lb - 1 (middle of the window)
	checkIdx := n - 1 - lb
	candidate := state.candles[checkIdx]

	// check if swing high
	isSwingHigh := false
	for i := checkIdx - lb; i <= checkIdx+lb; i++ {
		if i == checkIdx {
			continue
		}
		if state.candles[i].High >= candidate.High {
			isSwingHigh = false
			break
		}
	}

	if isSwingHigh {
		// avoid duplicates - check if we already recorded this swing
		if !s.hasSwingAt(state.swingHighs, candidate.CloseTs) {
			state.swingHighs = append(state.swingHighs, swingPoint{
				price:  candidate.High,
				ts:     candidate.CloseTs,
				isHigh: true,
			})
		}

		if len(state.swingHighs) > maxSwings {
			state.swingHighs = state.swingHighs[len(state.swingHighs)-maxSwings:]
		}
	}

	// check if swing low
	isSwingLow := true
	for i := checkIdx - lb; i <= checkIdx+lb; i++ {
		if i == checkIdx {
			continue
		}
		if state.candles[i].Low <= candidate.Low {
			isSwingLow = false
			break
		}
	}

	if isSwingLow {
		if !s.hasSwingAt(state.swingLows, candidate.CloseTs) {
			state.swingLows = append(state.swingLows, swingPoint{
				price:  candidate.Low,
				ts:     candidate.CloseTs,
				isHigh: false,
			})
		}
		if len(state.swingLows) > maxSwings {
			state.swingLows = state.swingLows[len(state.swingLows)-maxSwings:]
		}
	}
}

func (s *BuyTheDip) hasSwingAt(swings []swingPoint, ts int64) bool {
	for _, sp := range swings {
		if sp.ts == ts {
			return true
		}
	}
	return false
}

// evaluateTrend determines trend maturity based on:
// - EarlyTrend: n consecutive higher closes
// - ConfirmedTrend: at least cfg.ConfirmedSwings higher-highs AND higher-lows
// - MatureTrend: at least cfg.MatureSwings higher-highs AND higher-lows
func (s *BuyTheDip) evaluateTrend(state *symbolState) TrendMaturity {
	// count higher-highs and higher-lows
	hhCount := s.countHigherHighs(state.swingHighs)
	hlCount := s.countHigherLows(state.swingLows)

	// mature takes precedence
	if hhCount >= s.cfg.MatureSwings && hlCount >= s.cfg.MatureSwings {
		return MatureTrend
	}

	// confirmed trend
	if hhCount >= s.cfg.ConfirmedSwings && hlCount >= s.cfg.ConfirmedSwings {
		return ConfirmedTrend
	}

	// early trend based on consecutive closes
	if state.consecutiveUp >= s.cfg.EarlyTrendCloses {
		return EarlyTrend
	}

	return NoTrend
}

// countHigherHighs counts consecutive higher-highs from recent swings
func (s *BuyTheDip) countHigherHighs(swings []swingPoint) int {
	if len(swings) < 2 {
		return 0
	}

	count := 0
	for i := 1; i < len(swings); i++ {
		if swings[i].price > swings[i-1].price {
			count++
		} else {
			// reset on lower high, we want the most recent higher highs seq
			count = 0
		}
	}
	return count
}

// countHigherLows counts consecutive higher-lows from recent swings
func (s *BuyTheDip) countHigherLows(swings []swingPoint) int {
	if len(swings) < 2 {
		return 0
	}

	count := 0
	for i := 1; i < len(swings); i++ {
		if swings[i].price > swings[i-1].price {
			count++
		} else {
			// reset on lower low
			count = 0
		}
	}
	return count
}
