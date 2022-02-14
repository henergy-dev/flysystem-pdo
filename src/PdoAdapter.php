<?php

namespace Phlib\Flysystem\Pdo;

use League\Flysystem\FilesystemAdapter;
use League\Flysystem\Config;
use Phlib\Flysystem\Pdo\Util;
use League\Flysystem\FileAttributes;
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use League\Flysystem\Visibility;

class PdoAdapter implements FilesystemAdapter
{
    /**
     * @var \PDO
     */
    protected $db;

    /**
     * @var Config
     */
    protected $config;

    /**
     * @var string
     */
    protected $pathTable;

    /**
     * @var string
     */
    protected $chunkTable;

    /**
     * PdoAdapter constructor.
     * @param \PDO $db
     * @param Config $config
     */
    public function __construct(\PDO $db, Config $config = null)
    {
        $this->db = $db;

		if ($config === null) {
            $config = new Config;
        }
        $defaultPrefix = 'flysystem';
        $this->config = $config->withDefaults([
            'table_prefix'            => $defaultPrefix,
            'enable_compression'      => true,
            'chunk_size'              => 1048576, // 1MB chunks, in bytes
            'temp_dir'                => sys_get_temp_dir(),
            'disable_mysql_buffering' => true
        ]);

        $tablePrefix = trim($this->config->get('table_prefix'));
        if ($tablePrefix == '') {
            $tablePrefix = $defaultPrefix;
        }
        $this->pathTable  = "{$tablePrefix}_path";
        $this->chunkTable = "{$tablePrefix}_chunk";

        // if ($config->get('disable_mysql_buffering')) {
        //     $this->db->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        // }
    }

    /**
     * @inheritdoc
     */
    // public function write($path, $contents, Config $config)
	public function write(string $path, string $contents, Config $config): void
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $contents);

		$data = $this->findPathData($path);

		if (!is_array($data) || $data['type'] != 'file') {
        	$res = $this->doWrite($path, $filename, $contents, $resource, $config);
		}else{
			$res = $this->doUpdate($path, $filename, $contents, $resource, $config);
		}
		return;
    }

    /**
     * @inheritdoc
     */
    // public function writeStream($path, $resource, Config $config)
	public function writeStream(string $path, $contents, Config $config): void
    {
        $filename = $this->getTempFilename();
        $resource = $this->getTempResource($filename, $resource);

		$data = $this->findPathData($path);

        if (!is_array($data) || $data['type'] != 'file') {
            $res = $this->doWrite($path, $filename, '', $resource, $config);
        }else{
			$res = $this->doUpdate($path, $filename, '', $resource, $config);
		}



		return;
    }

    /**
     * @param string $path
     * @param string $filename
     * @param string $contents
     * @param resource $resource
     * @param Config $config
     * @return array|false
     */
    protected function doWrite($path, $filename, $contents, $resource, Config $config)
    {
		$mimeTypeDetector = new FinfoMimeTypeDetector();

        $enableCompression = (bool)$config->get('enable_compression', $this->config->get('enable_compression'));
        $data              = [
            'path'          => $path,
            'type'          => 'file',
            'mimetype'      => $mimeTypeDetector->detectMimeType($path, $contents) ?? 'text/plain',
            'visibility'    => $config->get('visibility', Visibility::PUBLIC),
            'size'          => filesize($filename),
            'is_compressed' => (int)$enableCompression
        ];
        $expiry = null;
        if ($config->get('expiry')) {
            $expiry = $data['expiry'] = $config->get('expiry');
        }
        $meta = null;
        if ($config->get('meta')) {
            $meta = $data['meta'] = $config->get('meta');
        }

        $data['path_id'] = $this->insertPath(
            'file',
            $data['path'],
            $data['visibility'],
            $data['mimetype'],
            $data['size'],
            $enableCompression,
            $expiry,
            $meta
        );
        if ($data['path_id'] === false) {
            $this->cleanupTemp($resource, $filename);
            return false;
        }

        $this->insertChunks($data['path_id'], $resource, $enableCompression);
        $this->cleanupTemp($resource, $filename);

        $data['update_ts'] = date('Y-m-d H:i:s');
        return $this->normalizeMetadata($data);
    }

    /**
     * @param string $path
     * @param string $filename
     * @param string $contents
     * @param resource $resource
     * @param Config $config
     * @return array|false
     */
    protected function doUpdate($path, $filename, $contents, $resource, Config $config)
    {
        $data = $this->findPathData($path);
        if (!is_array($data) || $data['type'] != 'file') {
            return false;
        }

		$mimeTypeDetector = new FinfoMimeTypeDetector();

        $searchKeys       = ['size', 'mimetype'];
        $data['size']     = filesize($filename);
        $data['mimetype'] = $mimeTypeDetector->detectMimeType($data['path'], $contents)  ?? 'text/plain';
        if ($config->get('expiry')) {
            $data['expiry'] = $config->get('expiry');
            $searchKeys[] = 'expiry';
        }
        if ($config->get('meta')) {
            $data['meta'] = json_encode($config->get('meta'));
            $searchKeys[] = 'meta';
        }

        $values = array_intersect_key($data, array_flip($searchKeys));
        $setValues = implode(', ', array_map(function ($field) {
            return "{$field} = :{$field}";
        }, array_keys($values)));

        $update = "UPDATE {$this->pathTable} SET {$setValues} WHERE path_id = :path_id";
        $stmt   = $this->db->prepare($update);
        $params = array_merge($values, ['path_id' => $data['path_id']]);
        if (!$stmt->execute($params)) {
            return false;
        }

        $this->deleteChunks($data['path_id']);
        $this->insertChunks($data['path_id'], $resource, (bool)$data['is_compressed']);
        $this->cleanupTemp($resource, $filename);

        $data['update_ts'] = date('Y-m-d H:i:s');
        return $this->normalizeMetadata($data);
    }

    /**
     * @inheritdoc
     */
    // public function rename($path, $newPath)
	public function move(string $path, string $newPath, Config $config): void
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return;
        }

        $update = "UPDATE {$this->pathTable} SET path = :newpath WHERE path_id = :path_id";
        $stmt   = $this->db->prepare($update);

        // rename the primary node first
        if (!$stmt->execute(['newpath' => $newPath, 'path_id' => $data['path_id']])) {
            return;
        }

        // rename all children when it's directory
        if ($data['type'] == 'dir') {
            $pathLength = strlen($path);
            $listing    = $this->listContents($path, true);
            foreach ($listing as $item) {
                $newItemPath = $newPath . substr($item['path'], $pathLength);
                $stmt->execute(['newpath' => $newItemPath, 'path_id' => $item['path_id']]);
            }
        }

        $data['path'] = $newPath;
        return; // $this->normalizeMetadata($data);
    }

    /**
     * @inheritdoc
     */
    public function copy(string $source, string $destination, Config $config): void
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return;
        }

        $newData = $data;
        $newData['path'] = $newPath;
        unset($newData['path_id']);
        unset($newData['update_ts']);

        $newData['path_id'] = $this->insertPath(
            $data['type'],
            $newData['path'],
            $data['visibility'],
            $data['mimetype'],
            $data['size'],
            $data['is_compressed'],
            isset($data['expiry']) ? $data['expiry'] : null,
            isset($data['meta']) ? $data['meta'] : null
        );

        if ($newData['type'] == 'file') {
            $resource = $this->getChunkResource($data['path_id'], (bool)$data['is_compressed']);
            $this->insertChunks($newData['path_id'], $resource, (bool)$data['is_compressed']);
            $this->cleanupTemp($resource, '');
        }

        $newData['update_ts'] = date('Y-m-d H:i:s');
        return;
    }

    /**
     * @inheritdoc
     */
    public function delete(string $path): void
    {
        $data = $this->findPathData($path);
        if (!is_array($data) || $data['type'] != 'file') {
            return;
        }

        if (!$this->deletePath($data['path_id'])) {
            return;
        }
        $this->deleteChunks($data['path_id']);

        return;
    }

    /**
     * @inheritdoc
     */
    public function deleteDirectory($dirname): void
    {
        $data = $this->findPathData($dirname);
        if (!is_array($data) || $data['type'] != 'dir') {
            return;
        }

        $listing = $this->listContents($dirname, true);

        foreach ($listing as $item) {
            $this->deletePath($item['path_id']);
            if ($item['type'] == 'file') {
                $this->deleteChunks($item['path_id']);
            }
        }

        return;
    }

    /**
     * @inheritdoc
     */
    public function createDirectory($dirname, Config $config): void
    {
        $additional = null;
        if ($config->get('meta')) {
            $additional = $config->get('meta');
        }
        $pathId = $this->insertPath('dir', $dirname, null, null, null, true, null, $additional);
        if ($pathId === false) {
            return;
        }

        $data = [
            'type'      => 'dir',
            'path'      => $dirname,
            'path_id'   => $pathId,
            'update_ts' => date('Y-m-d H:i:s')
        ];
        if ($additional !== null) {
            $data['meta'] = json_encode($additional);
        }
        return;
    }

    /**
     * @inheritdoc
     */
    public function setVisibility(string $path, string $visibility): void
    {
        $update = "UPDATE {$this->pathTable} SET visibility = :visibility WHERE path = :path";
        $data = ['visibility' => $visibility, 'path' => $path];
        $stmt = $this->db->prepare($update);
        if (!$stmt->execute($data)) {
            return;
        }

        return;
    }

    /**
     * @inheritdoc
     */
    public function fileExists($path): bool
    {
        //$select = "SELECT 1 FROM {$this->pathTable} WHERE path = :path LIMIT 1";
        $select = "SELECT TOP 1 1 FROM {$this->pathTable} WHERE path = :path";
        $stmt   = $this->db->prepare($select);
        if (!$stmt->execute(['path' => $path])) {
            return false;
        }

        return (bool)$stmt->fetchColumn();
    }

	/**
     * @inheritdoc
     */
    public function directoryExists($path): bool
    {
        //$select = "SELECT 1 FROM {$this->pathTable} WHERE path = :path LIMIT 1";
        $select = "SELECT TOP 1 1 FROM {$this->pathTable} WHERE path = :path";
        $stmt   = $this->db->prepare($select);
        if (!$stmt->execute(['path' => $path])) {
            return false;
        }

        return (bool)$stmt->fetchColumn();
    }

    /**
     * @inheritdoc
     */
    public function read($path): string
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return false;
        }

        $resource = $this->getChunkResource($data['path_id'], $data['is_compressed']);

        $metadata = $this->normalizeMetadata($data);
        $metadata['contents'] = stream_get_contents($resource);

        fclose($resource);

        return $metadata['contents'];
    }

    /**
     * @inheritdoc
     */
    public function readStream($path)
    {
        $data = $this->findPathData($path);
        if (!is_array($data)) {
            return false;
        }

        $metadata = $this->normalizeMetadata($data);
        $metadata['stream'] = $this->getChunkResource($metadata['path_id'], (bool)$data['is_compressed']);
        return $metadata['stream'];
    }

    /**
     * @param int $pathId
     * @param bool $isCompressed
     * @return resource
     */
    protected function getChunkResource($pathId, $isCompressed)
    {
        $resource = fopen('php://temp', 'w+b');
        $compressFilter = null;
        if ($isCompressed) {
            $compressFilter = stream_filter_append($resource, 'zlib.inflate', STREAM_FILTER_WRITE);
        }

        $this->extractChunks($pathId, $resource);

        if (is_resource($compressFilter)) {
            stream_filter_remove($compressFilter);
        }

        return $resource;
    }

    /**
     * @param int $pathId
     * @param resource $resource
     */
    protected function extractChunks($pathId, $resource)
    {
        $select = "SELECT content FROM {$this->chunkTable} WHERE path_id = :path_id ORDER BY chunk_no ASC";
        $stmt   = $this->db->prepare($select);
        $stmt->execute(['path_id' => $pathId]);
        while ($content = $stmt->fetchColumn()) {
            $contentLength = strlen($content);
            $pointer = 0;
            while ($pointer < $contentLength) {
                $pointer += fwrite($resource, substr($content, $pointer, 1024));
            }
            unset($content);
        }
        rewind($resource);
    }

    /**
     * @inheritdoc
     */
    // public function listContents($directory = '', $recursive = false)
	public function listContents(string $directory = '', bool $recursive = false): iterable
    {
        $params = [];
        $select = "SELECT * FROM {$this->pathTable}";

        if (!empty($directory)) {
            $select .= " WHERE path LIKE :prefix OR path = :path";
            $params = ['prefix' => $directory . '/%', 'path' => $directory];
        }

        $stmt = $this->db->prepare($select);
        if (!$stmt->execute($params)) {
            return [];
        }

        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $rows = array_map([$this, 'convertToFileAttribute'], $rows);
        if ($recursive) {
            $rows = Util::emulateDirectories($rows);
        }

        return $rows;
    }

	protected function convertToFileAttribute($data)
    {
		return new FileAttributes(
            $data['path'],
            $data['size'],
            $data['visibility'],
            strtotime($data['update_ts'])
        );
    }

    /**
     * @inheritdoc
     */
    public function getMetadata($path)
    {
        return $this->normalizeMetadata($this->findPathData($path));
    }

    /**
     * @inheritdoc
     */
    public function fileSize($path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'size');
    }

    /**
     * @inheritdoc
     */
    public function mimeType($path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'mimetype');
    }

    /**
     * @inheritdoc
     */
    public function lastModified($path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'timestamp');
    }

    /**
     * @inheritdoc
     */
    public function visibility($path): FileAttributes
    {
        return $this->getFileMetadataValue($path, 'visibility');
    }

    /**
     * @param string $path
     * @return array|false
     */
    protected function findPathData($path)
    {
        //$select = "SELECT * FROM {$this->pathTable} WHERE path = :path LIMIT 1";
        $select = "SELECT TOP 1 * FROM {$this->pathTable} WHERE path = :path";
        $stmt   = $this->db->prepare($select);
        if (!$stmt->execute(['path' => $path])) {
            return false;
        }

        $data = $stmt->fetch(\PDO::FETCH_ASSOC);
        if ($this->hasExpired($data)) {
            return false;
        }

        return $data;
    }

    /**
     * @param array $data
     * @return array|bool
     */
    protected function normalizeMetadata($data)
    {
        if (!is_array($data) || empty($data) || $this->hasExpired($data)) {
            return false;
        }

        $meta = [
            'path_id'   => $data['path_id'],
            'type'      => $data['type'],
            'path'      => $data['path'],
            'timestamp' => strtotime($data['update_ts'])
        ];
        if ($data['type'] == 'file') {
            $meta['mimetype']   = $data['mimetype'];
            $meta['size']       = $data['size'];
            $meta['visibility'] = $data['visibility'];
            if (isset($data['expiry'])) {
                $meta['expiry'] = $data['expiry'];
            }
        }

        if (isset($data['meta'])) {
            $meta['meta'] = json_decode($data['meta'], true);
        }

        return $meta;
    }

    /**
     * @param array $data
     * @return bool
     */
    protected function hasExpired($data)
    {
        if (isset($data['expiry']) &&
            !empty($data['expiry']) &&
            strtotime($data['expiry']) !== false &&
            strtotime($data['expiry']) <= time()
        ) {
            return true;
        }

        return false;
    }

    /**
     * @param string $path
     * @param string $property
     * @return array|false
     */
    protected function getFileMetadataValue($path, $property = null)
    {
        $meta = $this->getMetadata($path);
        if ($meta['type'] != 'file' || !isset($meta[$property])) {
            return false;
        }
        // return [$property => $meta[$property]];

		return new FileAttributes(
            $path,
            $meta['size'],
            $meta['visibility'],
            $meta['timestamp']
        );
    }

    /**
     * @param string $type 'file' or 'dir'
     * @param string $path
     * @param string $visibility 'public' or 'private'
     * @param string $mimeType
     * @param int $size
     * @param bool $enableCompression
     * @param string $expiry
     * @param array $additional
     * @return bool|string
     */
    protected function insertPath(
        $type,
        $path,
        $visibility = null,
        $mimeType = null,
        $size = null,
        $enableCompression = true,
        $expiry = null,
        $additional = null
    ) {
        $data = [
            'type'          => $type == 'dir' ? 'dir' : 'file',
            'path'          => $path,
            'visibility'    => $visibility,
            'mimetype'      => $mimeType,
            'size'          => $size,
            'is_compressed' => (int)(bool)$enableCompression
        ];
        if ($expiry !== null) {
            $data['expiry'] = $expiry;
        }
        if ($additional !== null) {
            $data['meta'] = json_encode($additional);
        }

        $keys = array_keys($data);
        $fields = implode(', ', $keys);
        $values = implode(', ', array_map(function ($field) {
            return ':' . $field;
        }, $keys));

        $insert = "INSERT INTO {$this->pathTable} ({$fields}) VALUES ({$values})";
        $stmt   = $this->db->prepare($insert);
        if (!$stmt->execute($data)) {
            return false;
        }

        return $this->db->lastInsertId();
    }

    /**
     * @param string|null $now Timestamp in expected format for query
     * @return int Number of expired files deleted
     */
    public function deleteExpired($now = null)
    {
        if ($now === null) {
            $now = date('Y-m-d H:i:s');
        }

        $select = "SELECT path_id FROM {$this->pathTable} WHERE expiry <= :now";
        $stmt = $this->db->prepare($select);
        $stmt->execute(['now' => $now]);

        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($rows as $row) {
            $this->deletePath($row['path_id']);
        }

        return $stmt->rowCount();
    }

    /**
     * @param int $pathId
     * @return bool
     */
    protected function deletePath($pathId)
    {
        $delete = "DELETE FROM {$this->pathTable} WHERE path_id = :path_id";
        $stmt   = $this->db->prepare($delete);
        return (bool)$stmt->execute(['path_id' => (int)$pathId]);
    }

    /**
     * @param int $pathId
     * @param resource $resource
     * @param bool $enableCompression
     */
    protected function insertChunks($pathId, $resource, $enableCompression)
    {
        rewind($resource);

        $compressFilter = null;
        if ($enableCompression) {
            $compressFilter = stream_filter_append($resource, 'zlib.deflate', STREAM_FILTER_READ);
        }

        $insert = "INSERT INTO {$this->chunkTable} (path_id, chunk_no, content) VALUES";
        // $insert .= " (:path_id, :chunk_no, :content)";
        $insert .= " (:path_id, :chunk_no, CONVERT(VARBINARY(MAX), :content))";

        $stmt      = $this->db->prepare($insert);
        $chunk     = 0;
        $chunkSize = $this->config->get('chunk_size');
        while (!feof($resource)) {
            $content = stream_get_contents($resource, $chunkSize);
            // when an empty stream is compressed it produces \000
            if ($content == '' || bin2hex($content) == '0300') {
                continue;
            }

            $stmt->execute([
                'path_id'  => $pathId,
                'chunk_no' => $chunk++,
                'content'  => $content
            ]);
        }

        if (is_resource($compressFilter)) {
            stream_filter_remove($compressFilter);
        }
    }

    /**
     * @param int $pathId
     * @return bool
     */
    protected function deleteChunks($pathId)
    {
        $delete = "DELETE FROM {$this->chunkTable} WHERE path_id = :path_id";
        $stmt   = $this->db->prepare($delete);
        return (bool)$stmt->execute(['path_id' => $pathId]);
    }

    /**
     * @return string
     */
    protected function getTempFilename()
    {
        $tempDir = $this->config->get('temp_dir');
        return tempnam($tempDir, "flysystempdo");
    }

    /**
     * @param string $filename
     * @param string|resource $content
     * @return resource
     */
    protected function getTempResource($filename, $content)
    {
        $resource = fopen($filename, 'w+b');
        if (!is_resource($content)) {
            fwrite($resource, (string)$content);
        } else {
            while (!feof($content)) {
                fwrite($resource, stream_get_contents($content, 1024), 1024);
            }
        }
        rewind($resource);

        return $resource;
    }

    /**
     * @param resource $resource
     * @param string $filename
     */
    protected function cleanupTemp($resource, $filename)
    {
        if (is_resource($resource)) {
            fclose($resource);
        }
        if (is_file($filename)) {
            unlink($filename);
        }
    }
}
