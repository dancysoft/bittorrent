
(dolist (file '("bittorrent-common.cl"
		"bencoding.cl"
		"bittorrent.cl"))
  (load (compile-file-if-needed file)))

	  