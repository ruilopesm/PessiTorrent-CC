package main

import (
	"PessiTorrent/internal/config"
	"PessiTorrent/internal/logger"
	"flag"
	"strconv"
)

func main() {
	cfg, err := config.NewConfig(config.DefaultConfigPath)
	if err != nil {
		logger.Error("Failed to load config: %s", err)
		return
	}

	dns := cfg.DNS.Host + ":" + strconv.FormatUint(uint64(cfg.DNS.Port), 10)
	trackerAddr := cfg.Tracker.Host + ":" + strconv.Itoa(int(cfg.Tracker.Port))
	udpPort := cfg.Node.Port

	flag.StringVar(&trackerAddr, "t", trackerAddr, "Tracker address")
	flag.UintVar(&udpPort, "p", udpPort, "Node UDP port")
	flag.Parse()

	node := NewNode(trackerAddr, uint16(udpPort), dns)
	node.Start()
}
