(in-package :user)

(eval-when (compile load eval)
  (require :aserve)
  (use-package :net.aserve.client))

(defparameter *blocksize* (* 16 1024))
;; http://wiki.theory.org/BitTorrentSpecification says that
;; 5 is a good value for 32KB blocksizes... so 10 must be a good 
;; value for 16KB blocksizes  [for a 5mbps link with 50ms latency]
(defparameter *max-outstanding-requests* 10)

(defparameter *max-upload-rate* 20) ;; KB/sec

(defmacro setf-bit (place bit)
  `(setf ,place (logior ,place (ash 1 ,bit))))

(defmacro clearf-bit (place bit)
  `(setf ,place (logandc2 ,place (ash 1 ,bit))))

(defstruct torrent
  (lock (mp:make-process-lock))
  files ;; list of 'f' structs 
  basedir 
  num-pieces
  piece-length 
  pieces 
  announce
  info-hash
  id
  (have 0)
  total-size
  (uploaded 0)
  (downloaded 0)
  peers
  blocks-per-piece
  seed
  (period-timestamp (get-universal-time))
  (uploaded-this-period 0)
  (downloaded-this-period 0)
  (upload-rate-lock (mp:make-process-lock)))

(defstruct f 
  length
  path
  stream)

(defstruct filepiece
  file
  file-offset
  len
  piece-offset)

(defstruct piece
  hash
  filepieces
  buf
  (have 0) ;; bitfield
  (requested 0)) ;; bitfield

(defstruct request
  index
  offset
  len)

(defstruct peer
  sock
  id ;; may not be available (or relevant)
  addr
  port
  have
  (haves-reported 0)
  status
  sent-requests
  (choked t)
  (interested nil)
  (choking-us t)
  (interested-in-us nil)
  (bytes-received-this-period 0)
  do-unchoke)


(defun peer-name (peer)
  (format nil "~a:~d" (peer-addr peer) (peer-port peer)))

(defun make-info-hash (info)
  (octets-to-string (sha1-string (bencode info) :return :usb8)
		    :end 20 :external-format :latin1))

(defun open-torrent (filename)
  (let* ((dict (decode-bencoding (file-contents filename :element-type 'usb8)))
	 (info (dict-get "info" dict))
	 (tor (make-torrent :announce (dict-get "announce" dict)
			    :id (make-id)
			    :info-hash (make-info-hash info)
			    :piece-length (dict-get "piece length" info))))

    (setf (torrent-num-pieces tor) (/ (length (dict-get "pieces" info)) 20))
    (setf (torrent-blocks-per-piece tor)
      (/ (torrent-piece-length tor) *blocksize*))
    
    (let ((files (dict-get "files" info)))
      (if* files
	 then ;; multi-file torrent
	      (setf (torrent-basedir tor)
		(clean-filename (dict-get "name" info)))
	      (let ((total 0)
		    res)
		(dolist (file files)
		  (push (make-f :length (dict-get "length" file)
				:path (mapcar #'clean-filename (dict-get "path" file)))
			res)
		  (incf total (dict-get "length" file)))
		(setf (torrent-files tor) (nreverse res))
		(setf (torrent-total-size tor) total))
	 else ;; single file torrent
	      (setf (torrent-basedir tor) ".")
	      (setf (torrent-files tor)
		    (list (make-f :length (dict-get "length" info)
				  :path (list (dict-get "name" info)))))
	      (setf (torrent-total-size tor) (dict-get "length" info))))

    (compute-pieces tor (dict-get "pieces" info))
    
    (let ((fresh (prep-torrent-files tor)))
      (if* (not fresh)
	 then (construct-have tor)))

    (open-torrent-files tor)
    
    tor))

(defun open-torrent-files (tor)
  (dolist (f (torrent-files tor))
    (setf (f-stream f)
      (open (f-filename tor f) :direction :io
	    :if-exists :overwrite))))

(defun close-torrent (tor)
  (dolist (f (torrent-files tor))
    (close (f-stream f))
    (setf (f-stream f) nil)))

(defmacro with-torrent ((tor filename) &body body)
  `(let ((,tor (open-torrent ,filename)))
     (unwind-protect (progn ,@body)
       (close-torrent ,tor))))

;; Acceptable filename chars.  Should be good on unix and windows.
;; 0-9, a-z, A-Z, '.', '_', and '-'. (posix portable filename)
(defun valid-filename-char-p (char)
  (or (alphanumericp char) (char= char #\.)
      (char= char #\_) (char= char #\-)))

(defun clean-filename (filename)
  ;; Check for particularly suspicious activities
  (if (string= filename ".")
      (error "Invalid filename '.' in torrent file"))
  (if (string= filename "..")
      (error "Invalid filename '..' in torrent file"))
  (let ((res (copy-seq filename)))
    (dotimes (n (length res))
      (if (not (valid-filename-char-p (schar res n)))
	  (setf (schar res n) #\_)))
    res))

(defun list-to-filename (comps)
  (list-to-delimited-string comps #\/))

(defun prep-file (base f)
  (let ((comps (list base))
	(path (f-path f))
	(len (f-length f)))
    (while (> (length path) 1)
      (push (pop path) comps)
      (let ((dirname (list-to-filename (reverse comps))))
	(if* (not (probe-file dirname))
	   then (format t "making ~a/~%" dirname)
		(make-directory dirname))))
    (push (pop path) comps)
    (let ((filename (list-to-filename (reverse comps))))
      (if* (probe-file filename)
	 then (if (/= len (file-length filename))
		  (error 
		   "~a exists but it is not the right size (~d). Aborting" filename
		   len))
	 else (format t "making ~a~%" filename)
	      (with-open-file (f filename :direction :output)
		(file-position f (1- len))
		(write-byte 0 f))))))

(defun prep-torrent-files (tor)
  (let ((base (torrent-basedir tor))
	(fresh nil))
    (when (not (probe-file base))
      (format t "making ~a/~%" base)
      (make-directory base)
      (setf fresh t))
    (dolist (f (torrent-files tor))
      (prep-file base f))
    fresh))

(defun make-id ()
  (concatenate 'string "-GG0100-"
	       (let ((tmp (make-string 12)))
		 (dotimes (n 12)
		   (setf (schar tmp n)
		     (code-char (+ (random 10) #.(char-code #\0)))))
		 tmp)))

(defun f-filename (tor f)
  (list-to-delimited-string (cons (torrent-basedir tor) (f-path f)) #\/))

(defun construct-have (tor)
  (format t "Scanning files...~%")
  (let* ((have 0)
	 (piece-length (torrent-piece-length tor))
	 (pieces (torrent-pieces tor))
	 (buf (make-usb8 piece-length))
	 (pos 0)
	 (index 0))
    (dolist (file (torrent-files tor))
      (with-open-file (f (f-filename tor file))
	(loop
	  (let ((stop (read-vector buf f :start pos)))
	    (if (= stop pos)
		(return)) ;; EOF
	    (if* (= stop piece-length)
	       then (if* (equalp (sha1-string buf :return :usb8) 
				 (piece-hash (aref pieces index)))
		       then (write-char #\*)
			    (setf-bit have index)
		       else (write-char #\.))
		    (finish-output)
		    (incf index)
		    (setf pos 0)
	       else (setf pos stop))))))
    ;; Compute the final hash (if there is one)
    (if* (not (zerop pos))
       then (if* (equalp (sha1-string buf :end pos :return :usb8) 
			 (piece-hash (aref pieces index)))
	       then (write-char #\*)
		    (setf-bit have index)
	       else (write-char #\.)))
    
    (format t "~%Scan complete.~%")
    
    (setf (torrent-have tor) have)
    (when (= (logcount have) (torrent-num-pieces tor))
      (format t "Seeding.~%")
      (setf (torrent-seed tor) t))))

#+ignore
(defun compute-file-locations (tor)
  (let* ((pos 0)
	 (index 0)
	 (piece-length (torrent-piece-length tor)))
    
    (dolist (file (torrent-files tor))
      (let* ((endpos (+ pos (f-length file)))
	     (endindex (truncate (/ endpos piece-length))))
	(setf (f-start-piece file) index)
	(setf (f-start-piece-offset file) (mod pos piece-length))
	(setf index endindex)
	(setf pos endpos)))))

(defun compute-pieces (tor pieces-string)
  (let* ((files (torrent-files tor))
	 (file (pop files))
	 (num-pieces (torrent-num-pieces tor))
	 (pieces (make-array num-pieces))
	 (file-offset 0)
	 (file-bytes-remaining (f-length file))
	 (piece-length (torrent-piece-length tor))
	 (string-pos 0))

    (if (not (zerop (mod (length pieces-string) 20)))
	(error "hash list is not a multiple of 20"))
    
    (dotimes (n num-pieces)
      (let ((piece-offset 0)
	    (piece-bytes-remaining piece-length)
	    fps)
	(loop
	  (let* ((used (min file-bytes-remaining piece-bytes-remaining))
		 (fp (make-filepiece :file file
				     :file-offset file-offset
				     :len used
				     :piece-offset piece-offset)))
	    (push fp fps)
	    (incf file-offset used)
	    (incf piece-offset used)
	    (decf file-bytes-remaining used)
	    (decf piece-bytes-remaining used)
	    
	    (when (zerop file-bytes-remaining)
	      (when (setf file (pop files))
		(setf file-offset 0)
		(setf file-bytes-remaining (f-length file))))
	    
	    (if (or (null file) (zerop piece-bytes-remaining))
		(return))))
	
	(setf (aref pieces n) 
	  (make-piece 
	   :filepieces (nreverse fps)
	   :hash (string-to-octets (subseq pieces-string string-pos
					   (+ string-pos 20))
				   :null-terminate nil
				   :external-format :latin1)))
	(incf string-pos 20)))

    (setf (torrent-pieces tor) pieces)))

(defun probe-tracker (tor &key event)
  (let ((data (do-http-request (torrent-announce tor)
		:query `(("info_hash" . ,(torrent-info-hash tor))
			 ("peer_id" . ,(torrent-id tor))
			 ("port" . "2706") 
			 ("uploaded" . ,(torrent-uploaded tor))
			 ("downloaded" . ,(torrent-downloaded tor))
			 ("left" . ,(- (torrent-total-size tor)
				       (torrent-downloaded tor)))
			 ("compact" . "1")
			 ,@(if event `("event" . ,event))))))
    (setf data (decode-bencoding data))
    (if (dict-get "failure reason" data)
	(error "Tracker reported failure: ~a" (dict-get "failure reason" data)))
    (if (dict-get "warning message" data)
	(format t "Tracker reported warning: ~a" (dict-get "warning message" data)))
    (format t "Interval: ~a seconds~%" (dict-get "interval" data))
    (if (dict-get "min interval" data)
	(format t "Min interval: ~a seconds~%" (dict-get "min interval" data)))
    (if (dict-get "tracker id" data)
	(format t "Tracker ID: ~a~%" (dict-get "tracker id" data)))
    (format t "Seeders: ~s~%" (dict-get "complete" data))
    (format t "Leechers: ~s~%" (dict-get "incomplete" data))
    (let ((peers (dict-get "peers" data)))
      (if* (stringp peers)
	 then ;; compact format
	      (parse-compact-peers peers)
       elseif (listp peers)
	 then ;; full format
	      (parse-peers peers)
	 else (error "Unexpected peers data: ~s" peers)))))
    
(defun parse-peers (peers)
  (let (res)
    (dolist (peer peers)
      (push (make-peer :id (dict-get "peer id" peer)
		       :addr (dict-get "ip" peer)
		       :port (dict-get "port" peer))
	    res))
    res))

(defun parse-compact-peers (peers)
  (if (not (zerop (rem (length peers) 6)))
      (error "compact peers list has invalid format (not a multiple of 6)"))
  (macrolet ((getbyte (pos)
	       `(char-code (schar peers ,pos))))
    (let ((n 0)
	  (max (length peers))
	  res)
      (while (< n max)
	(push (make-peer :addr 
			 (socket:ipaddr-to-dotted
			  (logior (ash (getbyte (+ 0 n)) 24)
				  (ash (getbyte (+ 1 n)) 16)
				  (ash (getbyte (+ 2 n)) 8)
				  (ash (getbyte (+ 3 n)) 0)))
			 :port (logior (ash (getbyte (+ 4 n)) 8)
				       (ash (getbyte (+ 5 n)) 0)))
	      res)
	(incf n 6))
      res)))

(defun do-handshake (peer tor stream)
  (let ((proto "BitTorrent protocol"))
    (write-byte (length proto) stream)
    (write-string proto stream)
    ;; reserved bytes
    (dotimes (n 8)
      (write-byte 0 stream))
    ;; info-hash
    (write-string (torrent-info-hash tor) stream)
    ;; peer-id
    (write-string (torrent-id tor) stream)
    (finish-output stream))
  
  (let* ((pstrlen (read-byte stream))
	 (pstr (make-string pstrlen))
	 (info-hash (make-string 20))
	 (peer-id (make-string 20)))
    (dotimes (n pstrlen)
      (setf (schar pstr n) (read-char stream)))
    ;;(format t "Peer protocol: ~a~%" pstr)
    ;; reserved bytes
    (dotimes (n 8)
      (read-byte stream))
    ;; info-hash
    (dotimes (n 20)
      (setf (schar info-hash n) (read-char stream)))
    ;; peer-id
    (dotimes (n 20)
      (setf (schar peer-id n) (read-char stream)))
    (setf (peer-id peer) peer)
    
    (if* (string= info-hash (torrent-info-hash tor))
       then ;;(format t "info-hash matches.~%")
	    t
       else ;;(format t "info-hash does not match.~%")
	    nil)))

(defmacro with-socket-1 ((sock) &body body)
  `(unwind-protect (progn ,@body)
     (ignore-errors (close ,sock))
     (ignore-errors (close ,sock :abort t))))

(defun bitfield-to-bytes (tor)
  (declare (optimize (speed 3) (safety 0)))
  (let* ((have (torrent-have tor))
	 (num-bits (torrent-num-pieces tor))
	 (len (ceiling (/ num-bits 8)))
	 (buf (make-usb8 len :initial-element 0))
	 (mask #x80)
	 (pos 0)
	 (byte 0))
    (declare (usb8 mask byte)
	     (ausb8 buf)
	     (fixnum num-bits len pos))
    (dotimes (n num-bits)
      (if (logbitp n have)
	  (setf byte (logior byte mask)))
      (when (zerop (setf mask (ash mask -1)))
	(setf mask #x80)
	(setf (aref buf pos) byte)
	(incf pos)
	(setf byte 0)))
    
    buf))

(defun write-complete-vector (vec stream &key (start 0) (end (length vec)))
  (while (< start end)
    (let ((stop (write-vector vec stream :start start :end end)))
      (if (= stop start)
	  (error "write failed"))
      (setf start stop))))

(defun read-complete-vector (vec stream &key (start 0) (end (length vec)))
  (while (< start end)
    (let ((stop (read-vector vec stream :start start :end end)))
      (when (= stop start)
	(mp:process-kill sys:*current-process*)
	#+ignore(error "Unexpected EOF"))
      (setf start stop))))

(defun get-uint32 (vec)
  (declare (optimize (speed 3) (safety 0))
	   (ausb8 vec))
  ;; For best compilation, the order here matters.
  (logior (logior 
	   (ash (aref vec 1) 16)
	   (ash (aref vec 2) 8)
	   (aref vec 3))
	  (ash (aref vec 0) 24)))

(defun set-uint32 (vec value)
  (declare (optimize (speed 3) (safety 0))
	   (ausb8 vec))
  (setf (aref vec 0) (logand #xff (ash value -24)))
  (setf (aref vec 1) (logand #xff (ash value -16)))
  (setf (aref vec 2) (logand #xff (ash value -8)))
  (setf (aref vec 3) (logand #xff value)))


(defun write-uint32 (stream value)
  (declare (optimize (speed 3)))
  (let ((buf (make-usb8 4)))
    (declare (dynamic-extent buf))
    (set-uint32 buf value)
    (write-complete-vector buf stream)))

(defun read-uint32 (stream)
  (declare (optimize (speed 3)))
  (let ((buf (make-usb8 4)))
    (declare (dynamic-extent buf))
    (read-complete-vector buf stream)
    (get-uint32 buf)))

;; call with lock held
(defun write-bitfield (stream bytes)
  (write-uint32 stream (1+ (length bytes)))
  (write-byte 5 stream) ;; 'bitfield' message
  (write-vector bytes stream)
  (finish-output stream))

(defun read-bitfield (stream len tor)
  (let ((buf (make-usb8 len))
	(have 0)
	(mask 0)
	(pos 0)
	byte)
    (read-complete-vector buf stream)
    (dotimes (n (torrent-num-pieces tor))
      (if* (zerop mask)
	 then (setf mask #x80)
	      (setf byte (aref buf pos))
	      (incf pos))
      (if* (not (zerop (logand byte mask)))
	 then (setf-bit have n))
      (setf mask (ash mask -1)))
    have))

;; Call with torrent locked.
(defun compute-wants-bitfield (peer tor)
  (let ((have (peer-have peer)))
    (if* have 
       then (logandc1 (torrent-have tor) (peer-have peer))
       else 0)))

(defun compute-piece-len (tor index)
  (let ((nominal-len (torrent-piece-length tor))
	(total (torrent-total-size tor)))
    (if* (< (* (1+ index) nominal-len) total)
       then nominal-len
       else (mod total nominal-len))))

;; Returns the number of blocks and the length of the last block.
;; For all but the last piece, this will be 
;; torrent-blocks-per-piece and *blocksize*
(defun compute-piece-info (tor index)
  (let* ((piece-len (compute-piece-len tor index))
	 (blocks (ceiling (/ piece-len *blocksize*))))
    (dotimes (n (1- blocks))
      (decf piece-len *blocksize*))
    (values blocks piece-len)))

;; Call with torrent locked.
(defmacro piece-bitfield-full-p (bf max-pop)
  `(= (logcount ,bf) ,max-pop))

;; Call with torrent locked
(defun find-incomplete-wanted-piece (wants tor)
  (dotimes (n (torrent-num-pieces tor))
    (when (logbitp n wants)
      (let ((piece (aref (torrent-pieces tor) n))
	    (blocks (compute-piece-info tor n)))
	(if (not (piece-bitfield-full-p (piece-requested piece) blocks))
	    (return n))))))

(defun index-of-first-unset-bit (value)
  (let ((n 0))
    (while (logbitp n value)
      (incf n))
    n))

(defun alloc-bit (value)
  (let ((index (index-of-first-unset-bit value)))
    (values index (logior value (ash 1 index)))))

;; Call with torrent locked
(defun alloc-request-1 (tor index)
  (let ((piece (aref (torrent-pieces tor) index)))
    (multiple-value-bind (blocks last-block-len)
	(compute-piece-info tor index)
      (multiple-value-bind (blocknum requested)
	  (alloc-bit (piece-requested piece))
	;; Sanity check.
	(if (>= blocknum blocks)
	    (error "allocated more than the available # of blocks"))
	(setf (piece-requested piece) requested)
      
	(make-request :index index
		      :offset (* *blocksize* blocknum)
		      :len (if* (= (1+ blocknum) blocks)
			      then last-block-len
			      else *blocksize*))))))

(defun alloc-and-submit-request (sock tor peer)
  (let* ((wants (compute-wants-bitfield peer tor))
	 (index (find-incomplete-wanted-piece wants tor)))
    (if index
	(let ((req (alloc-request-1 tor index)))
	  ;;(format t "Allocated request: ~a~%" req)
	  (push req (peer-sent-requests peer))
	  (submit-request sock req)
	  t))))

(defun submit-request (sock req)
  (write-uint32 sock 13)
  (write-byte 6 sock) ;; request
  (write-uint32 sock (request-index req))
  (write-uint32 sock (request-offset req))
  (write-uint32 sock (request-len req))
  (finish-output sock))


(defun express-interest (peer sock)
  ;;(format t "Expressing interest to ~a~%" (peer-name peer))
  (write-uint32 sock 1)
  (write-byte 2 sock) ;; interested
  (finish-output sock)
  (setf (peer-interested peer) t))


(defun express-disinterest (peer sock)
  ;;(format t "Expressing disinterest to ~a~%" (peer-name peer))
  (write-uint32 sock 1)
  (write-byte 3 sock) ;; not interested
  (finish-output sock)
  (setf (peer-interested peer) nil))

(defun process-bitfield-msg (peer msglen tor sock)
  ;;(format t "BITFIELD~%")
  (if (peer-have peer)
      (error "peer sent bitfield message but we already have it"))
  (if (/= msglen (ceiling (/ (torrent-num-pieces tor) 8)))
      (error "peer sent bitfield message with invalid length"))
		   
  (setf (peer-have peer) (read-bitfield sock msglen tor)))


(defun process-have-msg (peer msglen tor sock)
  (if (/= msglen 4)
      (error "invalid HAVE message length"))
  (let ((index (read-uint32 sock))
	(have (or (peer-have peer) 0)))
    ;;(format t "HAVE ~d~%" index)
    
    (if (>= index (torrent-num-pieces tor))
	(error "Invalid index"))

    (setf-bit have index)
    (setf (peer-have peer) have)))

(defun process-piece-msg (peer msglen tor sock)
  (decf msglen 8)
  (if (< msglen 1)
      (error "invalid piece data len: ~d~%" msglen))
  (let* ((index (read-uint32 sock))
	 (begin (read-uint32 sock))
	 (req (find-matching-req peer index begin msglen)))
    #+ignore
    (format t "~a: PIECE ~d, o: ~d, l: ~d~%" 
	    (peer-name peer) index begin msglen)
    (if* (null req)
       then ;; Unexpected data.  Discard.
	    (format t "Not expecting this piece.  Discarding.~%")
	    (dotimes (n msglen)
	      (read-byte sock))
       else (let ((piece (aref (torrent-pieces tor) index))
		  buf)
	      (mp:with-process-lock ((torrent-lock tor))
		(setf buf (or (piece-buf piece)
			      (setf (piece-buf piece)
				(make-usb8 (compute-piece-len tor index))))))
	      (read-complete-vector buf sock 
				    :start begin :end (+ begin msglen))
	      (incf (peer-bytes-received-this-period peer) msglen)
	      (mp:with-process-lock ((torrent-lock tor))
		(incf (torrent-downloaded-this-period tor) msglen)
		(finish-request req piece peer tor))))))

(defun find-matching-req (peer index begin len)
  (dolist (req (peer-sent-requests peer))
    (if (and (= (request-index req) index)
	     (= (request-offset req) begin)
	     (= (request-len req) len))
	(return req))))

(defun finish-request (req piece peer tor)
  (mp:with-process-lock ((torrent-lock tor))
    (let* ((blocknum (/ (request-offset req) *blocksize*))
	   (index (request-index req))
	   (blocks (compute-piece-info tor index)))
      (setf (peer-sent-requests peer) 
	(delete req (peer-sent-requests peer)))
      ;;(format t "finish-request, block: ~d~%" blocknum)
      ;;(clearf-bit (piece-requested piece) blocknum)
      (setf-bit (piece-have piece) blocknum)
      
      ;; check to see if we have the whole piece now.
      (when (piece-bitfield-full-p (piece-have piece) blocks)
	;;(format t "Verifying piece ~a~%" index)
	(if* (verify-piece piece)
	   then (format t "*** Piece ~a verified ***~%" index)
		(write-piece piece)
		(setf (piece-buf piece) nil)
		(setf-bit (torrent-have tor) index)
		(if (= (logcount (torrent-have tor)) (torrent-num-pieces tor))
		    (setf (torrent-seed tor) t))
	   else ;; Verify failed
		(format t "Verify failed.  Discarding piece.~%")
		(setf (piece-requested piece) 0)
		(setf (piece-have piece) 0))))))
		      

(defun verify-piece (piece)
  (equalp (piece-hash piece) (sha1-string (piece-buf piece) :return :usb8)))

;; Call with torrent locked
(defun write-piece (piece)
  (let ((buf (piece-buf piece))
	(pos 0))
    (dolist (fp (piece-filepieces piece))
      (let* ((file (filepiece-file fp))
	     (stream (f-stream file))
	     (len (filepiece-len fp)))
	(file-position stream (filepiece-file-offset fp))
	(write-vector buf stream :start pos :end (+ pos len))
	(finish-output stream)
	(incf pos len)))))

(defun cancel-outstanding-sent-requests (peer tor)
  ;; Cancel any outstanding requests made to this peer.
  (mp:with-process-lock ((torrent-lock tor))
    (dolist (req (peer-sent-requests peer))
      ;;(format t "Removing request: ~a~%" req)
      (let* ((index (request-index req))
	     (piece (aref (torrent-pieces tor) index))
	     (blocknum (/ (request-offset req) *blocksize*)))
	(clearf-bit (piece-requested piece) blocknum)))
    (setf (peer-sent-requests peer) nil)))
  

(defun process-choke-msg (peer tor)
  (format t "~a: CHOKE~%" (peer-name peer))
  (setf (peer-choking-us peer) t)
  (cancel-outstanding-sent-requests peer tor))

(defun process-interest-changes (peer tor sock)
  (let ((wants (mp:with-process-lock ((torrent-lock tor))
		 (compute-wants-bitfield peer tor))))
    (if* (and (zerop wants) (peer-interested peer))
       then (express-disinterest peer sock)
     elseif (and (not (zerop wants)) (not (peer-interested peer)))
       then (express-interest peer sock))))

(defun maybe-submit-requests (peer tor sock)
  (when (and (peer-interested peer) (not (peer-choking-us peer)))
    (while (and (< (length (peer-sent-requests peer))
		   *max-outstanding-requests*)
		(alloc-and-submit-request sock tor peer)))))

(defun report-haves (peer tor sock)
  (let* ((haves (mp:with-process-lock ((torrent-lock tor))
		  (torrent-have tor)))
	 (reported (peer-haves-reported peer))
	 (unreported (logandc2 haves reported))
	 ;; report stuff in unreported that is not in peer-have
	 (to-report (logandc2 unreported (or (peer-have peer) 0))))
    (when (not (zerop to-report))
      (dotimes (n (torrent-num-pieces tor))
	(when (logbitp n to-report)
	  ;;(format t "Reporting HAVE ~d to ~a~%" n (peer-name peer))
	  (write-uint32 sock 5)
	  (write-byte 4 sock) ;; HAVE 
	  (write-uint32 sock n)
	  (setf-bit (peer-haves-reported peer) n)))
      (finish-output sock))))

(defmacro with-socket-error-handler (() &body body)
  `(handler-case (progn ,@body)
     (socket-error (c)
       (declare (ignore c)))
     (errno-stream-error (c)
       (if (/= (stream-error-code c) excl::*epipe*)
	   (error c)))))

(defmacro with-peer-connection ((peer tor) &body body)
  `(unwind-protect 
       (with-socket-error-handler () ,@body)
     ;; cleanup
     (format t "~a disconnecting.~%" (peer-name ,peer))
     (cancel-outstanding-sent-requests ,peer ,tor)
     (mp:with-process-lock ((torrent-lock tor))
       (setf (torrent-peers tor)
	 (delete peer (torrent-peers tor))))))

;; TODO: send keep-alive every 2 minutes.
;; TODO: Disconnect from seeds if we're a seed as well.
;; TODO: Use rarest-first download strategy.
(defun handle-connection (sock tor peer)
  (with-socket-1 (sock)
    (when (not (ignore-errors (do-handshake peer tor sock)))
      (setf (peer-status peer) :handshake-failed)
      (return-from handle-connection))
    
    (setf (peer-status peer) :connected)
    
    ;; Initialize
    (setf (peer-have peer) nil)
    (setf (peer-sent-requests peer) nil)
    (setf (peer-choked peer) t)
    (setf (peer-interested peer) nil)
    (setf (peer-choking-us peer) t)
    (setf (peer-interested-in-us peer) nil)
    
    ;; If we have at least one piece, transmit our bitmap.
    (let (have bytes)
      (mp:with-process-lock ((torrent-lock tor))
	(setf have (torrent-have tor))
	(if (not (zerop have))
	    (setf bytes (bitfield-to-bytes tor))))
      (when bytes
	;;(format t "Transmitting our bitmap.~%")
	(write-bitfield sock bytes)
	(setf (peer-haves-reported peer) have)))

    (with-peer-connection (peer tor)
      (loop
	(handler-case (process-interest-changes peer tor sock)
	  (socket-error (c)
	    (declare (ignore c))
	    (setf (peer-status peer) :hung-up)
	    (return))
	  (error (c)
	    (error c)))
	    
	(maybe-submit-requests peer tor sock)
	(report-haves peer tor sock)

	(when (mp:wait-for-input-available sock :timeout 5)
	  (let ((msglen (ignore-errors (read-uint32 sock)))
		msg)
	    (if* (null msglen)
	       then ;; Peer hung up.
		    (setf (peer-status peer) :hung-up)
		    (return)
	     elseif (= msglen 0)
	       then ;;(format t "Got keep-alive message.~%")
		    nil
	       else (setf msg (read-byte sock))
		    (decf msglen)
		    (case msg
		      (0 (process-choke-msg peer tor))
		      (1 ;;(format t "UNCHOKE~%")
		       (setf (peer-choking-us peer) nil))
		      (2 (format t "~a: INTERESTED~%" (peer-name peer))
		       (setf (peer-interested-in-us peer) t))
		      (3 (format t "~a: NOT INTERESTED~%" (peer-name peer))
		       (setf (peer-interested-in-us peer) nil))
		      (4 (process-have-msg peer msglen tor sock))
		      (5 (process-bitfield-msg peer msglen tor sock))
		      (6 (process-request-msg peer msglen tor sock))
		      (7 (process-piece-msg peer msglen tor sock))
		      (8 (process-cancel-msg peer msglen tor sock))
		      (t (error "Unsupported message type: ~d" msg))))))))))

;; we don't do anything with this because we don't process requests
;; asynchronously 
(defun process-cancel-msg (peer msglen tor sock)
  (declare (ignore peer tor))
  (if (/= msglen 12)
      (error "Invalid cancel message len"))
  (let* ((index (read-uint32 sock))
	 (offset (read-uint32 sock))
	 (length (read-uint32 sock)))
    index
    offset
    length
    #+ignore
    (format t "CANCEL i: ~d, o: ~d, l: ~d~%" index offset length)))
	 

(defun process-request-msg (peer msglen tor sock)
  (if (/= msglen 12)
      (error "Invalid request message len."))
  (let* ((index (read-uint32 sock))
	 (offset (read-uint32 sock))
	 (length (read-uint32 sock))
	 plen)

    #+ignore
    (format t "~a: REQUEST i: ~d, o: ~d, l: ~d~%"
	    (peer-name peer) index offset length)
    
    (if (> length #.(* 128 1024))
	(error "Request length of ~d is too large" length))
    (if (>= index (torrent-num-pieces tor))
	(error "Request for invalid index: ~d" index))
    (setf plen (compute-piece-len tor index))
    (if (>= offset plen)
	(error "Request for invalid piece offset: ~d" offset))
    (if (> (+ offset length) plen)
	(error "Request for invalid section of piece (offset+len exceeds size of piece)"))
    
    ;; Only heed requests that were supplied by unchoked peers.
    (when (not (peer-choked peer))
      (send-piece peer tor index offset length))))

(defun get-current-upload-rate (tor)
  (mp:with-process-lock ((torrent-lock tor))
    (let* ((now (get-universal-time))
	   (elapsed (- now (torrent-period-timestamp tor))))
      (when (zerop elapsed)
	(sleep 1)
	(incf elapsed 1))
      (/ (torrent-uploaded-this-period tor) 1024.0 elapsed))))

;; The lock ensures that there is only one busy waiter.
(defun wait-for-upload-transfer-rate (tor)
  (mp:with-process-lock ((torrent-upload-rate-lock tor))
    (while (> (get-current-upload-rate tor) *max-upload-rate*)
      ;;(format t "rating for upload rate to drop...~%")
      (sleep 1))))
  

(defun send-piece (peer tor index offset length)
  (let* ((sock (peer-sock peer))
	 (piece (aref (torrent-pieces tor) index))
	 (fps (piece-filepieces piece))
	 (fp (pop fps)))

    (wait-for-upload-transfer-rate tor)
    
    #+ignore
    (format t "Sending piece ~d (~d/~d) to ~a~%" 
	    index offset length (peer-name peer))
    (write-uint32 sock (+ 9 length))
    (write-byte 7 sock)
    (write-uint32 sock index)
    (write-uint32 sock offset)

    ;; Find the filepiece which contains 'offset'.
    (while (>= offset (+ (filepiece-piece-offset fp) (filepiece-len fp)))
      (decf offset (filepiece-len fp))
      (setf fp (pop fps)))

    ;;(format t "starting with fp: ~s~%" fp)
    ;;(format t "offset within that file: ~d~%" offset)
    
    (while (> length 0)
      ;;(format t "bytes remaining to send: ~d~%" length)
      (let* ((count (min (- (filepiece-len fp) offset) length))
	     (stream (f-stream (filepiece-file fp)))
	     (buf (make-usb8 count)))
	;;(format t "want to read ~d bytes from fp.~%" count)
	(mp:with-process-lock ((torrent-lock tor))
	  (file-position stream (+ (filepiece-file-offset fp) offset))
	  (read-complete-vector buf stream :end count))
	(write-complete-vector buf sock :end count)
	(mp:with-process-lock ((torrent-lock tor))
	  (incf (torrent-uploaded-this-period tor) count)) 
	;;(format t "wrote ~d bytes to socket.~%" count)
	(decf length count)
	(setf fp (pop fps))
	(setf offset 0)
	(if (and fp (> length 0))
	    (format t "Moved to fp ~a, offset 0.~%" fp))))
    
    (finish-output sock)))
	

(defparameter *max-uploads* 4)

;; non-seed Alg:
;; Unchoke the (1- *max-uploads*) fastest interested uploaders.  This 
;; provides reciprocation

;; Unchoke uninterested uploaders who are faster than uploaders in the 
;; prior group.  This step may be unnecessary if we simply call
;; this alg any time interest changes (does that result in too much
;; work?)

;; choke everyone else (don't send choke message if they are already
;; choked).

;; TODO: When choking, kill off any pending requests they have made.

;; The above is not done yet.  For the time being, we're just going
;; to select the first *max-uploads* fastest interested uploaders.

;; Sorts by speed.  Call with torrent locked
(defun get-interested-peers (tor)
  (let (interested)
    (dolist (peer (torrent-peers tor))
      (if (and (eq (peer-status peer) :connected)
	       (peer-interested-in-us peer))
	  (push peer interested)))
    (sort interested 
	  #'(lambda (a b) (> (peer-bytes-received-this-period a)
			     (peer-bytes-received-this-period b))))))

(defun reset-received-counts (tor)
  (dolist (peer (torrent-peers tor))
    (setf (peer-bytes-received-this-period peer) 0)))

(defun choke (peer)
  (format t "Sending choke to ~a~%" (peer-name peer))
  (let ((sock (peer-sock peer)))
    (write-uint32 sock 1)
    (write-byte 0 sock) ;; choke
    (finish-output sock)
    (setf (peer-choked peer) t)))

(defun unchoke (peer)
  (format t "Sending unchoke to ~a~%" (peer-name peer))
  (let ((sock (peer-sock peer)))
    (write-uint32 sock 1)
    (write-byte 1 sock) ;; unchoke
    (finish-output sock)
    (setf (peer-choked peer) nil)
    (setf (peer-do-unchoke peer) nil)))

(defun choke-algorithm (tor)
  (mp:with-process-lock ((torrent-lock tor))
    (let ((interested (get-interested-peers tor))
	  (count 0))
      (dolist (peer interested)
	(if (>= count *max-uploads*)
	    (return))
	(incf count)
	;;(format t "Selected ~a for unchoke.~%" (peer-name peer))
	(setf (peer-do-unchoke peer) t)))
    
    (dolist (peer (torrent-peers tor))
      (if* (and (peer-do-unchoke peer) (peer-choked peer))
	 then (unchoke peer)
       elseif (and (not (peer-do-unchoke peer)) (not (peer-choked peer)))
	 then (choke peer)))
    
    (reset-received-counts tor)))

(defmacro with-socket ((sock &rest args) &body body)
  `(let ((,sock (socket:make-socket ,@args)))
     (unwind-protect
	 (progn ,@body)
       (close ,sock))))

(defmacro with-accept ((sock ss) &body body)
  `(let ((,sock (socket:accept-connection ,ss)))
     (unwind-protect
	 (progn ,@body)
       (close ,sock))))

(defun start-peer (tor peer)
  (setf (peer-status peer) :connecting)
  (multiple-value-bind (sock err)
      (ignore-errors (socket:make-socket :remote-host (peer-addr peer) 
					 :remote-port (peer-port peer)))
    (if* (null sock)
       then (format t "Connection to ~a failed: ~a~%" (peer-name peer) err)
	    (mp:with-process-lock ((torrent-lock tor))
	      (setf (torrent-peers tor) (delete peer (torrent-peers tor))))
       else (with-socket-1 (sock)
	      (setf (peer-sock peer) sock)
	      (format t "connection completed.~%")
	      (handle-connection sock tor peer)))))

;;TODO:
;; need an unwind protect that sends a stopped event to the tracker.
(defparameter tor nil)

;; TODO:
;; if useable peer set drops below some number (20), ask for 20 more 
;; peers (when leeching).  Add a limit on the number of peers.  (80)
;; Disallow multiple connections from the same client (based on
;; peer id  or peer id+address)
(defun bittorrent (file)
  (if (null tor)
      (setf tor (open-torrent file)))
  (if (and (null (torrent-peers tor)) (not (torrent-seed tor)))
      (setf (torrent-peers tor) (probe-tracker tor)))
  (mp:with-process-lock ((torrent-lock tor))
    (dolist (peer (torrent-peers tor))
      (if (null (peer-status peer))
	  (mp:process-run-function (format nil "peer ~a" (peer-name peer))
	    #'start-peer tor peer))))
  (mp:process-run-function "incoming connection handler"
    #'incoming-connection-handler tor)
  (loop
    (sleep 10)
    (choke-algorithm tor)
    (report tor)))

(defun incoming-connection-handler (tor)
  (let ((sock (socket:make-socket :connect :passive :local-port 2706
				  :reuse-address t)))
    (loop
      (let ((cli (ignore-errors (socket:accept-connection sock))))
	(when cli
	  (let ((peer (make-peer :sock cli
				 :addr (socket:ipaddr-to-dotted 
					(socket:remote-host cli))
				 :port (socket:remote-port cli))))
	    (format t "Incoming peer ~a~%" (peer-name peer))
	    (mp:with-process-lock ((torrent-lock tor))
	      (push peer (torrent-peers tor)))
	    
	    (mp:process-run-function 
		(format nil "incoming peer ~a" (peer-name peer))
	      #'handle-connection cli tor peer)))))))

(defun report (tor)
  (mp:with-process-lock ((torrent-lock tor))
    (let* ((now (get-universal-time))
	   (elapsed (- now (torrent-period-timestamp tor))))
      (format t "Upload rate: ~a KB/sec~%" 
	      (/ (torrent-uploaded-this-period tor) 1024.0 elapsed))
      (format t "Download rate: ~a KB/sec~%" 
	      (/ (torrent-downloaded-this-period tor) 1024.0 elapsed))
		 
      (setf (torrent-period-timestamp tor) now)
      (setf (torrent-uploaded-this-period tor) 0)
      (setf (torrent-downloaded-this-period tor) 0)
      t)))
      
