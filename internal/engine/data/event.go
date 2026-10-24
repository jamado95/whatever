package data

// Status is a frame's kind; Late and Backfill follow with the live and warm-up paths.
type Status uint8

const Current Status = iota // the newest column, released for the first time

/*
evaluate tells readiness that a direct stream has decided its due columns up to col: Fresh or Gap at
col, or Ended from col on — col −1 when the lane ended without a candle.
*/
type evaluate struct {
	stream int
	col    int64
	state  LegState
}
