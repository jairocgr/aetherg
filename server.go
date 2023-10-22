package main

import (
	"container/list"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type AetherSettings struct {
	Host          string
	Port          int
	Replicate     bool
	SourceAddress string
	Snapshot      string
}

type AetherServer struct {
	host          string
	port          int
	hm            *hashmap
	listener      net.Listener
	clients       clientList
	replicas      *clientSet
	events        chan event
	snapFile      string
	snapshotting  bool
	snapSync      sync.RWMutex
	replicate     bool
	sourceAddress string
	master        *master
	nextId        int64
	creation      time.Time
	statistics    *ioStatistics
	eventCount    int
	network       ioStats
	disk          ioStats
}

const version = "v0.1.0-beta"

const maxClientsAllowed = 512

func (s *AetherServer) newEvent(e event) {
	s.events <- e
}

type clientSet struct {
	clients map[string]*aetherClient
}

func (s *clientSet) add(c *aetherClient) {
	s.clients[c.getId()] = c
}

func (s *clientSet) broadcast(c *command) {
	response := newBroadcastCommandResponse(c)
	for _, client := range s.clients {
		client.enqueueReply(response)
	}
}

func (s *clientSet) rm(client *aetherClient) {
	delete(s.clients, client.getId())
}

func (s *clientSet) count() int {
	return len(s.clients)
}

type clientList struct {
	clients list.List
}

type ioStats struct {
	In     int `json:"in"`
	Out    int `json:"out"`
	Reads  int `json:"reads"`
	Writes int `json:"writes"`
}

func (s *ioStats) average(secs int) {
	s.In = s.In / secs
	s.Out = s.Out / secs
	s.Reads = s.Reads / secs
	s.Writes = s.Writes / secs
}

func (stats *ioStats) add(event *ioEvent) {
	switch event.kind {
	case input:
		stats.In += event.data.bytes
		stats.Reads += event.data.calls
	case output:
		stats.Out += event.data.bytes
		stats.Writes += event.data.calls
	}
}

type ServerRole string

const Master ServerRole = "MASTER"
const ReadReplica ServerRole = "READ_REPLICA"

type connectionInfo struct {
	Id      string  `json:"id"`
	Address string  `json:"address"`
	Network ioStats `json:"network"`
}

type serverStats struct {
	Role        ServerRole       `json:"role"`
	Uptime      int              `json:"uptime"`
	Disk        ioStats          `json:"disk"`
	Network     ioStats          `json:"network"`
	Keys        int              `json:"keys"`
	Replicas    int              `json:"replicas"`
	Connections []connectionInfo `json:"connections"`
}

func NewAetherServer(settings AetherSettings) *AetherServer {
	return &AetherServer{
		host:          settings.Host,
		port:          settings.Port,
		hm:            newHashmap(),
		events:        make(chan event),
		replicas:      newClientSet(),
		snapFile:      absPath(settings.Snapshot),
		replicate:     settings.Replicate,
		sourceAddress: settings.SourceAddress,
		nextId:        genIdSeed(),
		statistics:    newIoStatistics(),
	}
}

func genIdSeed() int64 {
	currentHour := time.Now().Hour()
	seed := currentHour * 1000
	return int64(seed)
}

func newClientSet() *clientSet {
	return &clientSet{clients: make(map[string]*aetherClient)}
}

func (s *AetherServer) openServerSocket() {
	address := s.host + ":" + strconv.Itoa(s.port)
	server, err := net.Listen("tcp", address)
	if err != nil {
		log.WithFields(log.Fields{
			"host":  s.host,
			"port":  s.port,
			"error": err,
		}).Error("Error listening")
		os.Exit(EXIT_FAILURE)
	}

	log.WithFields(log.Fields{
		"host": s.host,
		"port": s.port,
	}).Info("Listening for new connections")

	s.listener = server
	s.creation = time.Now()
}

func (s *AetherServer) Run() {
	if s.isAReplica() {
		s.loadFromMasterNode()
	} else {
		// TODO: Before load, should clean up old temp snapshot that was left undone by ungraceful teardown
		s.loadSnapshot()
	}
	s.openServerSocket()
	go s.listenToSignals()
	go s.listenToNewConnections()
	go s.pacemaker()
	s.eventLoop()
}

func (s *AetherServer) nextClientId() string {
	defer s.incrNextId()
	return strconv.FormatInt(s.nextId, 10)
}

func (s *AetherServer) listenToSignals() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	s.events <- newSignalEvent(sig)
}

func (s *AetherServer) tearDown() {
	log.Warn("Tear down")
	s.closeSocket()

	if s.isAReplica() {
		s.master.close()
	}

	s.waitForSnapshot() // Wait if there is any
	if s.mustSave() {
		log.Warn("Snapshotting before exit")
		items := s.getItems()
		s.persist(items)
	}
}

func (s *AetherServer) add(c *aetherClient) {
	s.clients.add(c)
}

func (s *AetherServer) rm(c *aetherClient) {
	s.clients.rm(c)
}

func (s *AetherServer) getNumberOfClients() int {
	return s.clients.count()
}

func (s *AetherServer) getKeys() []string {
	return s.hm.getKeys()
}

func (s *AetherServer) getStats() serverStats {
	var stats serverStats

	if s.isAReplica() {
		stats.Role = ReadReplica
	} else {
		stats.Role = Master
	}

	stats.Network = s.network
	stats.Disk = s.disk

	stats.Uptime = s.getUptime()
	stats.Connections = s.clients.summarizeClients()
	stats.Keys = s.hm.count()
	stats.Replicas = s.replicas.count()
	return stats
}

func (s *AetherServer) logErrorClosingSocket(err error) {
	logError("Error closing server listening socket", err)
}

func (s *AetherServer) listenToNewConnections() {
	for {
		conn, err := s.listener.Accept()
		switch {
		case err != nil:
			s.events <- newErrorAcceptingConnectionEvent(err)
			return
		case s.reachedConnectionLimit():
			s.events <- newConnectionLimitReached(conn)
		default:
			s.logNewConnection(conn)
			s.events <- newConnEvent(conn)
		}
	}
}

func (s *AetherServer) eventLoop() {
	defer s.tearDown()
	for {
		event := <-s.events
		s.eventCount++
		stop := event.exec(s)
		if stop {
			return
		}
	}
}

func (s *AetherServer) logNewConnection(conn net.Conn) {
	addr := conn.RemoteAddr()
	log.WithFields(log.Fields{
		"network": addr.Network(),
		"address": addr.String(),
	}).Info("New connection accepted")
}

func (s *AetherServer) pacemaker() {
	var beat = 1
	for {
		time.Sleep(1 * time.Second)
		s.events <- newHeartBeat(beat)
		beat++
	}
}

func (s *AetherServer) getItems() []*item {
	return s.hm.getItens()
}

func (s *AetherServer) addReplica(c *aetherClient) {
	s.replicas.add(c)
}

func (s *AetherServer) broadcast(c *command) {
	s.replicas.broadcast(c)
}

func (s *AetherServer) disconnect(client *aetherClient) {
	defer client.close()
	s.clients.rm(client)
	s.replicas.rm(client)
	client.logExit()
}

func (s *AetherServer) evictExpiredKeys() {
	s.hm.evict()
}

func (s *AetherServer) loadSnapshot() {

	if !s.snapshotExists() {
		info("No snapshot file to load", log.Fields{"snapshot": s.snapFile})
		return
	}

	info("Loading snapshot file", log.Fields{"snapshot": s.snapFile})

	snap, err := os.Open(s.snapFile)
	if err != nil {
		fatalError("Error opening snapshot file", err)
	}

	src := newBufferedSource(snap, 4096)
	parser := newParser(src)

	for {
		command, _, err := parser.next()
		switch {
		case err == nil:
			switch command.getCode() {
			case commandSet:
				s.hm.set(command.getKey(), command.getValue(), command.getExpiration())
			default:
				fatal("Invalid command in snapshot file", log.Fields{"code": command.getCode()})
			}
		case err.isEOF():
			info("Snapshot loaded (EOF reached)", nil)
			s.hm.washClean()
			return
		case err != nil:
			fatalError("Error reading file", err)
		}
	}
}

func (s *AetherServer) dirty() bool {
	return s.hm.isDirty()
}

func (s *AetherServer) persist(items []*item) {
	defer s.setSnapshotting(false)
	start := time.Now()
	snap, err := s.genTempSnapshot()
	if err != nil {
		logError("Error opening temp snap file", err)
		os.Exit(EXIT_FAILURE)
	}

	defer removeFile(snap)

	logger := log.WithFields(log.Fields{
		"snapFile": s.snapFile,
		"tmp":      snap.Name(),
	})

	logger.Info("Creating snapshot")

	sink := newSink(snap, 4096)

	header := fmt.Sprintf("# aetherg %v snapshot %v\n", version, time.Now())
	sink.writeAsRawBytes(header)

	for _, item := range items {
		if item.isTransient() {
			continue // Transient items must not be persisted
		}
		pieces := item.genSetCommandPieces()
		sink.writeArrayOfProtocolStrings(pieces...)
		if sink.full() {
			data, err := sink.flush()
			write := newDiskWriteEvent(data)
			go s.newEvent(write)
			if err != nil {
				fatalError("Error writing to snapshot", err)
			}
		}
	}

	data, err := sink.flush()
	write := newDiskWriteEvent(data)
	go s.newEvent(write)
	if err != nil {
		fatalError("Error writing to snapshot", err)
	}

	err = os.Rename(snap.Name(), s.snapFile)
	if err != nil {
		fatalError("Error replacing snapshot with the new one", err)
	}

	logger.WithField("duration", time.Since(start)).Info("Snapshot is done")
}

func (s *AetherServer) snapshotExists() bool {
	return fileExists(s.snapFile)
}

func (s *AetherServer) setSnapshotting(val bool) {
	s.snapSync.Lock()
	defer s.snapSync.Unlock()
	s.snapshotting = val
}

func (s *AetherServer) isSnapshotting() bool {
	s.snapSync.RLock()
	defer s.snapSync.RUnlock()
	return s.snapshotting
}

func (s *AetherServer) washClean() {
	s.hm.washClean()
}

func (s *AetherServer) genTempSnapshot() (*os.File, error) {
	dir := filepath.Dir(s.snapFile)
	return os.CreateTemp(dir, "aetherg-*.tmp")
}

func (s *AetherServer) mustSave() bool {
	return !s.isAReplica() && s.dirty() && !s.isSnapshotting()
}

func (s *AetherServer) waitForSnapshot() {
	for s.isSnapshotting() {
		time.Sleep(1 * time.Second)
	}
}

func (s *AetherServer) closeSocket() {
	err := s.listener.Close()
	if err != nil {
		s.logErrorClosingSocket(err)
	}
}

func (s *AetherServer) isAReplica() bool {
	return s.replicate
}

func (s *AetherServer) loadFromMasterNode() {
	s.master = newMasterNode(s.sourceAddress)
	s.master.open()
	s.master.sync(s)
	go s.master.follow(s)
}

func (s *AetherServer) incrNextId() {
	s.nextId++
}

func (s *AetherServer) getUptime() int {
	return int(time.Now().Sub(s.creation).Seconds())
}

func (s *AetherServer) accountFor(io *ioEvent) {
	s.statistics.add(io)
}

func (s *AetherServer) updateStatistics() {
	s.network, s.disk = s.statistics.analyze()
}

func (s *AetherServer) reachedConnectionLimit() bool {
	return s.clients.count() == maxClientsAllowed
}

func (l *clientList) add(c *aetherClient) {
	l.clients.PushBack(c)
}

func (l *clientList) rm(c *aetherClient) {
	for e := l.clients.Front(); e != nil; e = e.Next() {
		if e.Value == c {
			l.clients.Remove(e)
			return
		}
	}
}

func (l *clientList) count() int {
	return l.clients.Len()
}

func (l *clientList) summarizeClients() []connectionInfo {
	conns := make([]connectionInfo, 0)
	for e := l.clients.Front(); e != nil; e = e.Next() {
		client := e.Value.(*aetherClient)
		conns = append(conns, connectionInfo{
			Id:      client.getId(),
			Address: client.getOriginAddr(),
			Network: client.getStats(),
		})
	}
	return conns
}
