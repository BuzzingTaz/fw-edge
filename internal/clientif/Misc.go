package clientif

import (
	"slices"
)

func IsValidProtocol(protocol string) bool {
	supportedProtocols := []string{"webrtc"}
	return slices.Contains(supportedProtocols, protocol)
}

