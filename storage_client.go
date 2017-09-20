package gfs

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
)

type (
	StorageClient struct {
		pool *ConnectionPool
	}
)

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 上传主文件
 * filename: 文件名
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageUploadByFilename(tc *TrackerClient,
	storeServ *StorageServer, filename string) (*UploadResponse, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := getFileExt(filename)

	return this.storageUploadFile(tc, storeServ, filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 上传主文件
 * fileBuffer: 字节数组
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageUploadByBuffer(tc *TrackerClient,
	storeServ *StorageServer, fileBuffer []byte, fileExtName string) (*UploadResponse, error) {
	bufferSize := len(fileBuffer)

	return this.storageUploadFile(tc, storeServ, fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 上传从文件
 * filename: 文件名
 * remoteFileId: 远程文件id（包含组名称）
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageUploadSlaveByFilename(tc *TrackerClient,
	storeServ *StorageServer, filename string, prefixName string, remoteFileId string) (*UploadResponse, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := getFileExt(filename)

	return this.storageUploadFile(tc, storeServ, filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
		STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, remoteFileId, prefixName, fileExtName)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 上传从文件
 * fileBuffer: 字节数组
 * remoteFileId: 远程文件id（包含组名称）
 * fileExtName: 扩展名
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageUploadSlaveByBuffer(tc *TrackerClient,
	storeServ *StorageServer, fileBuffer []byte, remoteFileId string, fileExtName string) (*UploadResponse, error) {
	bufferSize := len(fileBuffer)

	return this.storageUploadFile(tc, storeServ, fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
		STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, "", remoteFileId, fileExtName)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 更新数据
 * filename: 文件名
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageUploadAppenderByFilename(tc *TrackerClient,
	storeServ *StorageServer, filename string) (*UploadResponse, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := getFileExt(filename)

	return this.storageUploadFile(tc, storeServ, filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
		STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 更新数据
 * fileBuffer: 字节数组
 * fileExtName: 扩展名
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageUploadAppenderByBuffer(tc *TrackerClient,
	storeServ *StorageServer, fileBuffer []byte, fileExtName string) (*UploadResponse, error) {
	bufferSize := len(fileBuffer)

	return this.storageUploadFile(tc, storeServ, fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
		STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 上传文件
 * fileContent: 文件名 | 字节数组
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageUploadFile(tc *TrackerClient,
	storeServ *StorageServer, fileContent interface{}, fileSize int64, uploadType int,
	cmd int8, masterFilename string, prefixName string, fileExtName string) (*UploadResponse, error) {

	var conn net.Conn
	var uploadSlave bool
	var headerLen int64 = 15
	var reqBuf []byte
	var err error

	//获取链接
	conn, err = this.pool.Get()
	defer conn.Close()
	if err != nil {
		return nil, err
	}

	masterFilenameLen := int64(len(masterFilename))
	if len(storeServ.groupName) > 0 && len(masterFilename) > 0 {
		uploadSlave = true
		// #slave_fmt |-master_len(8)-file_size(8)-prefix_name(16)-file_ext_name(6)
		//       #           -master_name(master_filename_len)-|
		headerLen = int64(38) + masterFilenameLen
	}

	th := &trackerHeader{}
	th.pkgLen = headerLen
	th.pkgLen += int64(fileSize)
	th.cmd = cmd
	th.sendHeader(conn)

	if uploadSlave {
		req := &uploadSlaveFileRequest{}
		req.masterFilenameLen = masterFilenameLen
		req.fileSize = int64(fileSize)
		req.prefixName = prefixName
		req.fileExtName = fileExtName
		req.masterFilename = masterFilename
		reqBuf, err = req.marshal()
	} else {
		req := &uploadFileRequest{}
		req.storePathIndex = uint8(storeServ.storePathIndex)
		req.fileSize = int64(fileSize)
		req.fileExtName = fileExtName
		reqBuf, err = req.marshal()
	}

	if err != nil {
		log.Printf("uploadFileRequest.marshal error :%s", err.Error())
		return nil, err
	}

	//发送头部数据
	TcpSendData(conn, reqBuf)

	//发送文件数据
	switch uploadType {
	case FDFS_UPLOAD_BY_FILENAME:
		if filename, ok := fileContent.(string); ok {
			err = TcpSendFile(conn, filename)
		}
	case FDFS_DOWNLOAD_TO_BUFFER:
		if fileBuffer, ok := fileContent.([]byte); ok {
			err = TcpSendData(conn, fileBuffer)
		}
	}

	if err != nil {
		log.Print(err.Error())
		return nil, err
	}

	//接收返回头部
	th.recvHeader(conn)
	if th.status != 0 {
		return nil, Errno{int(th.status)}
	}

	recvBuff, recvSize, err := TcpRecvResponse(conn, th.pkgLen)
	if recvSize <= int64(FDFS_GROUP_NAME_MAX_LEN) {
		errmsg := "[-] Error: Storage response length is not match, "
		errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
		log.Print(errmsg)
		return nil, errors.New(errmsg)
	}

	//解析返回结果
	resp := &UploadResponse{}
	err = resp.unmarshal(recvBuff)
	if err != nil {
		errmsg := fmt.Sprintf("recvBuf can not unmarshal :%s", err.Error())
		log.Print(errmsg)
		return nil, errors.New(errmsg)
	}

	return resp, nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 删除文件
 * remoteFilename: 远程文件名（不含组名称）
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageDeleteFile(tc *TrackerClient, storeServ *StorageServer, remoteFilename string) error {
	var conn net.Conn
	var reqBuf []byte
	var err error

	//获取链接
	conn, err = this.pool.Get()
	defer conn.Close()
	if err != nil {
		return err
	}

	//发送头数据
	th := &trackerHeader{}
	th.cmd = STORAGE_PROTO_CMD_DELETE_FILE
	fileNameLen := len(remoteFilename)
	th.pkgLen = int64(FDFS_GROUP_NAME_MAX_LEN + fileNameLen)
	th.sendHeader(conn)

	//发送删除文件包
	req := &deleteFileRequest{}
	req.groupName = storeServ.groupName
	req.remoteFilename = remoteFilename
	reqBuf, err = req.marshal()
	if err != nil {
		log.Printf("deleteFileRequest.marshal error :%s", err.Error())
		return err
	}
	TcpSendData(conn, reqBuf)

	//接收头数据
	th.recvHeader(conn)
	if th.status != 0 {
		return Errno{int(th.status)}
	}

	return nil
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 下载到本地文件
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageDownloadToFile(tc *TrackerClient,
	storeServ *StorageServer, localFilename string, offset int64,
	downloadSize int64, remoteFilename string) (*DownloadResponse, error) {
	return this.storageDownloadFile(tc, storeServ, localFilename, offset, downloadSize, FDFS_DOWNLOAD_TO_FILE, remoteFilename)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 下载到字节数组
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageDownloadToBuffer(tc *TrackerClient,
	storeServ *StorageServer, fileBuffer []byte, offset int64,
	downloadSize int64, remoteFilename string) (*DownloadResponse, error) {
	return this.storageDownloadFile(tc, storeServ, fileBuffer, offset, downloadSize, FDFS_DOWNLOAD_TO_BUFFER, remoteFilename)
}

/* ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * 下载文件
 * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ */
func (this *StorageClient) storageDownloadFile(tc *TrackerClient,
	storeServ *StorageServer, fileContent interface{}, offset int64, downloadSize int64,
	downloadType int, remoteFilename string) (*DownloadResponse, error) {

	var conn net.Conn
	var reqBuf []byte
	var localFilename string
	var recvBuff []byte
	var recvSize int64
	var err error

	//获取链接
	conn, err = this.pool.Get()
	defer conn.Close()
	if err != nil {
		return nil, err
	}

	//发送头数据
	th := &trackerHeader{}
	th.cmd = STORAGE_PROTO_CMD_DOWNLOAD_FILE
	th.pkgLen = int64(FDFS_PROTO_PKG_LEN_SIZE*2 + FDFS_GROUP_NAME_MAX_LEN + len(remoteFilename))
	th.sendHeader(conn)

	//发送下载包数据
	req := &downloadFileRequest{}
	req.offset = offset
	req.downloadSize = downloadSize
	req.groupName = storeServ.groupName
	req.remoteFilename = remoteFilename
	reqBuf, err = req.marshal()
	if err != nil {
		log.Printf("downloadFileRequest.marshal error :%s", err.Error())
		return nil, err
	}
	TcpSendData(conn, reqBuf)

	//接收头数据
	th.recvHeader(conn)
	if th.status != 0 {
		return nil, Errno{int(th.status)}
	}

	//解析返回数据
	switch downloadType {
	case FDFS_DOWNLOAD_TO_FILE:
		if localFilename, ok := fileContent.(string); ok {
			recvSize, err = TcpRecvFile(conn, localFilename, th.pkgLen)
		}
	case FDFS_DOWNLOAD_TO_BUFFER:
		if _, ok := fileContent.([]byte); ok {
			recvBuff, recvSize, err = TcpRecvResponse(conn, th.pkgLen)
		}
	}

	if err != nil {
		log.Print(err.Error())
		return nil, err
	}

	if recvSize < downloadSize {
		errmsg := "[-] Error: Storage response length is not match, "
		errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
		log.Print(errmsg)
		return nil, errors.New(errmsg)
	}

	//解析返回结果
	resp := &DownloadResponse{}
	resp.RemoteFileId = storeServ.groupName + string(os.PathSeparator) + remoteFilename
	if downloadType == FDFS_DOWNLOAD_TO_FILE {
		resp.Content = localFilename
	} else {
		resp.Content = recvBuff
	}
	resp.Size = recvSize

	return resp, nil
}
