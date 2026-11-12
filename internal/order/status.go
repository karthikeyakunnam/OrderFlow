package order

import "errors"

var ErrInvalidStatus = errors.New("invalid order status")
var ErrInvalidTransition = errors.New("invalid order status transition")

var allowedStatuses = map[string]bool{
	"CREATED":    true,
	"CONFIRMED":  true,
	"PROCESSING": true,
	"SHIPPED":    true,
	"DELIVERED":  true,
	"CANCELLED":  true,
}

func isAllowedStatus(status string) bool {
	return allowedStatuses[status]
}

var allowedTransitions = map[string]map[string]bool{
	"CREATED": {
		"CONFIRMED": true,
		"CANCELLED": true,
	},
	"CONFIRMED": {
		"PROCESSING": true,
		"CANCELLED":  true,
	},
	"PROCESSING": {
		"SHIPPED":   true,
		"CANCELLED": true,
	},
	"SHIPPED": {
		"DELIVERED": true,
	},
}

func canTransition(from string, to string) bool {
	if from == to {
		return true
	}
	return allowedTransitions[from][to]
}
