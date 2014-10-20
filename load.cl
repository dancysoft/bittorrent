
(defvar *files* '("bittorrent-common.cl"
		  "bencoding.cl"
		  "bittorrent.cl"))
    
(dolist (file *files*)
  (load (compile-file-if-needed file)))

(let ((full-fasl "bittorrent_full.fasl"))
  (format t ";; Creating ~a...~%" full-fasl)
  (with-open-file (s full-fasl :direction :output
		   :if-exists :supersede)
    (dolist (file *files*)
      (let ((fasl (merge-pathnames #p(:type "fasl") file)))
	(sys:copy-file fasl s)))))
