package gfs

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

var (
	storagePoolChan      chan *storagePool          = make(chan *storagePool, 1)
	storagePoolMap       map[string]*ConnectionPool = make(map[string]*ConnectionPool)
	fetchStoragePoolChan chan interface{}           = make(chan interface{}, 1)
	quit                 chan bool
)

type (
	FdfsClient struct {
		server  *Server
		pool    *ConnectionPool
		timeout int
	}

	Server struct {
		Hosts []string
		Port  int
	}

	storagePool struct {
		key      string
		hosts    []string
		port     int
		minConns int
		maxConns int
	}
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	go func() {
		for {
			select {
			case spd := <-storagePoolChan:
				if sp, ok := storagePoolMap[spd.key]; ok {
					fetchStoragePoolChan <- sp
				} else {
					var (
						sp  *ConnectionPool
						err error
					)
					sp, err = NewConnectionPool(spd.hosts, spd.port, spd.minConns, spd.maxConns)
					if err != nil {
						fetchStoragePoolChan <- err
					} else {
						storagePoolMap[spd.key] = sp
						fetchStoragePoolChan <- sp
					}
				}
			case <-quit:
				break
			}
		}
	}()
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取Fdfs客户端
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func NewFdfsClient(hosts []string) (*FdfsClient, error) {
	server, err := getTrackerServer(hosts)
	if err != nil {
		return nil, err
	}

	pool, err := NewConnectionPool(server.Hosts, server.Port, 10, 150)
	if err != nil {
		return nil, err
	}

	return &FdfsClient{server: server, pool: pool}, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 关闭Fdfs客户端
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func ColseFdfsClient() {
	quit <- true
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 上传磁盘文件
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) UploadByFilename(filename string) (*UploadResponse, error) {
	if err := checkFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tc := &TrackerClient{this.pool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	storage := &StorageClient{storagePool}

	return storage.storageUploadByFilename(tc, storeServ, filename)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 上传文件数据
 * fileExtName: 文件扩展名
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) UploadByBuffer(filebuffer []byte, fileExtName string) (*UploadResponse, error) {
	tc := &TrackerClient{this.pool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	storage := &StorageClient{storagePool}

	return storage.storageUploadByBuffer(tc, storeServ, filebuffer, fileExtName)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 上传从磁盘文件数据
 * filename: 文件名
 * remoteFileId: 远程文件id
 * prefixName: 前缀
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) UploadSlaveByFilename(filename, remoteFileId, prefixName string) (*UploadResponse, error) {
	if err := checkFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.pool}
	storeServ, err := tc.trackerQueryStorageStorWithGroup(groupName)
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	storage := &StorageClient{storagePool}

	return storage.storageUploadSlaveByFilename(tc, storeServ, filename, prefixName, remoteFilename)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 上传从文件数据
 * filebuffer: 文件字节数组
 * remoteFileId: 远程文件id
 * prefixName: 前缀
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) UploadSlaveByBuffer(filebuffer []byte, remoteFileId, fileExtName string) (*UploadResponse, error) {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}

	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.pool}
	storeServ, err := tc.trackerQueryStorageStorWithGroup(groupName)
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	storage := &StorageClient{storagePool}

	return storage.storageUploadSlaveByBuffer(tc, storeServ, filebuffer, remoteFilename, fileExtName)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 更新磁盘文件
 * filename: 磁盘文件名
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) UploadAppenderByFilename(filename string) (*UploadResponse, error) {
	if err := checkFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tc := &TrackerClient{this.pool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	storage := &StorageClient{storagePool}

	return storage.storageUploadAppenderByFilename(tc, storeServ, filename)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 更新文件数据
 * filebuffer: 字节字节数组
 * fileExtName: 文件扩展名
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) UploadAppenderByBuffer(filebuffer []byte, fileExtName string) (*UploadResponse, error) {
	tc := &TrackerClient{this.pool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	storage := &StorageClient{storagePool}

	return storage.storageUploadAppenderByBuffer(tc, storeServ, filebuffer, fileExtName)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 删除文件
 * remoteFileId: 远程文件id
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) DeleteFile(remoteFileId string) error {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return err
	}

	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.pool}
	storeServ, err := tc.trackerQueryStorageUpdate(groupName, remoteFilename)
	if err != nil {
		return err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	storage := &StorageClient{storagePool}

	return storage.storageDeleteFile(tc, storeServ, remoteFilename)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 下载到本地文件
 * localFilename: 本地文件名
 * remoteFileId: 远程文件id
 * offset: 远程文件偏移量
 * size: 下载长度
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) DownloadToFile(localFilename string, remoteFileId string, offset int64, size int64) (*DownloadResponse, error) {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.pool}
	storeServ, err := tc.trackerQueryStorageFetch(groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	storage := &StorageClient{storagePool}

	return storage.storageDownloadToFile(tc, storeServ, localFilename, offset, size, remoteFilename)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 下载到字节数组
 * remoteFileId: 远程文件id
 * offset: 远程文件偏移量
 * size: 下载长度
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) DownloadToBuffer(remoteFileId string, offset int64, size int64) (*DownloadResponse, error) {
	tmp, err := splitRemoteFileId(remoteFileId)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{this.pool}
	storeServ, err := tc.trackerQueryStorageFetch(groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	storagePool, err := this.getStoragePool(storeServ.ipAddr, storeServ.port)
	storage := &StorageClient{storagePool}

	var fileBuffer []byte
	return storage.storageDownloadToBuffer(tc, storeServ, fileBuffer, offset, size, remoteFilename)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取StoragePool
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *FdfsClient) getStoragePool(ipAddr string, port int) (*ConnectionPool, error) {
	hosts := []string{ipAddr}
	var key string = fmt.Sprintf("%s-%d", hosts[0], port)
	var result interface{}
	var err error
	var ok bool

	spd := &storagePool{
		key:      key,
		hosts:    hosts,
		port:     port,
		minConns: 10,
		maxConns: 150,
	}
	storagePoolChan <- spd
	for {
		select {
		case result = <-fetchStoragePoolChan:
			var storagePool *ConnectionPool
			if err, ok = result.(error); ok {
				return nil, err
			} else if storagePool, ok = result.(*ConnectionPool); ok {
				return storagePool, nil
			} else {
				return nil, errors.New("none")
			}
		}
	}
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 获取TrackerServer
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func getTrackerServer(hosts []string) (*Server, error) {
	var trackerHosts []string
	var trackerPort string = "22122"

	for _, host := range hosts {
		var trackerIp string
		host = strings.TrimSpace(host)
		parts := strings.Split(host, ":")
		trackerIp = parts[0]
		if len(parts) == 2 {
			trackerPort = parts[1]
		}
		if trackerIp != "" {
			trackerHosts = append(trackerHosts, trackerIp)
		}
	}

	port, err := strconv.Atoi(trackerPort)
	if err != nil {
		return nil, err
	}

	tracerServer := &Server{
		Hosts: trackerHosts,
		Port:  port,
	}
	return tracerServer, nil
}
