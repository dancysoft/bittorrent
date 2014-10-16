(in-package :user)

(defconstant *action-connect* 0)
(defconstant *action-announce* 1)
(defconstant *action-scrape* 2)
(defconstant *action-error* 3) ;; not an actual action of course

(defconstant *event-none* 0)
(defconstant *event-completed* 1)
(defconstant *event-started* 2)
(defconstant *event-stopped* 3)

(defvar *our-id* (random (expt 2 160)))

(defun put160-net (buf pos value)
  (declare (optimize speed (safety 0))
	   (ausb8 buf)
	   (fixnum pos))
  (let ((offset 19))
    (declare (fixnum offset))
    (while (>= offset 0)
      (setf (aref buf (+ pos offset)) value)
      (setf value (ash value -8))
      (decf offset))))

(defun put64-net (buf pos value)
  (declare (optimize speed (safety 0))
	   (ausb8 buf)
	   (fixnum pos)
	   ((unsigned-byte 64) value))
  (let ((offset 7))
    (declare (fixnum offset))
    (while (>= offset 0)
      (setf (aref buf (+ pos offset)) value)
      (setf value (ash value -8))
      (decf offset))))

(defun put32-net (buf pos value)
  (declare (optimize speed (safety 0))
	   (ausb8 buf)
	   (fixnum pos)
	   ((unsigned-byte 32) value))
  (let ((offset 3))
    (declare (fixnum offset))
    (while (>= offset 0)
      (setf (aref buf (+ pos offset)) value)
      (setf value (ash value -8))
      (decf offset))))

(defun put16-net (buf pos value)
  (declare (optimize speed (safety 0))
	   (ausb8 buf)
	   (fixnum pos)
	   ((unsigned-byte 16) value))
  (let ((offset 1))
    (declare (fixnum offset))
    (while (>= offset 0)
      (setf (aref buf (+ pos offset)) value)
      (setf value (ash value -8))
      (decf offset))))

(defun get64-net (buf pos)
  (declare (optimize speed (safety 0))
	   (ausb8 buf)
	   (fixnum pos))
  (let ((value 0))
    (declare ((unsigned-byte 64) value))
    (dotimes (n 8)
      (setf value (logior (ash value 8) (aref buf (+ n pos)))))
    value))

(defun get32-net (buf pos)
  (declare (optimize speed (safety 0))
	   (ausb8 buf)
	   (fixnum pos))
  (let ((value 0))
    (declare ((unsigned-byte 32) value))
    (dotimes (n 4)
      (setf value (logior (ash value 8) (aref buf (+ n pos)))))
    value))

(defun get16-net (buf pos)
  (declare (optimize speed (safety 0))
	   (ausb8 buf)
	   (fixnum pos))
  (let ((value 0))
    (declare ((unsigned-byte 16) value))
    (dotimes (n 2)
      (setf value (logior (ash value 8) (aref buf (+ n pos)))))
    value))

(defun populate-udp-track-request-header (buf cid action tid)
  (declare (optimize speed (safety 0)))
  (put64-net buf 0 cid)
  (put32-net buf 8 action)
  (put32-net buf 12 tid))

(defun parse-udp-track-response-header (buf)
  "Returns action and transaction-id"
  (declare (optimize speed (safety 0)))
  (values (get32-net buf 0)
	  (get32-net buf 4)))

(defun random32 ()
  (random (expt 2 32)))

(defstruct (addr (:print-object print-addr))
  ip
  port
  )

(defun print-addr (addr stream)
  (format stream "~a:~a" (socket:ipaddr-to-dotted (addr-ip addr)) (addr-port addr)))
		   
(defun addr-matches-p (a b)
  (and (= (addr-ip a) (addr-ip b))
       (= (addr-port a) (addr-port b))))

(defstruct udp-tracker-host
  addr ;; struct
  cid 
  cid-expiration-time
  (retry-count 0)
  )

(defvar *udp-tracker-hosts* nil)
(defvar *udp-tracker-hosts-lock* (mp:make-process-lock :name "udp tracker hosts lock"))

(defun ensure-udp-tracker-host (addr)
  (mp:with-process-lock (*udp-tracker-hosts-lock*)
    (dolist (h *udp-tracker-hosts*)
      (when (addr-matches-p (udp-tracker-host-addr h) addr)
	;; Got it.
	(return-from ensure-udp-tracker-host h)))
    ;; Add a fresh entry
    (let ((h (make-udp-tracker-host :addr addr)))
      (push h *udp-tracker-hosts*)
      h)))


(defstruct pending-request
  addr
  tid
  (gate (mp:make-gate nil))
  buf
  size
  )

(defvar *pending-requests* nil)
(defvar *pending-requests-lock* (mp:make-process-lock :name "pending requests lock"))

(defun pending-request-matches-p (pr addr tid)
  (and (addr-matches-p (pending-request-addr pr) addr)
       (= (pending-request-tid pr) tid)))

(defun lookup-pending-request (addr tid)
  ;; Caller is responsible for locking
  (dolist (pr *pending-requests*)
    (when (pending-request-matches-p pr addr tid)
      (return pr))))

(defun lookup-and-remove-pending-request (addr tid)
  (mp:with-process-lock (*pending-requests-lock*)
    (let ((pr (lookup-pending-request addr tid)))
      (when pr
	(setf *pending-requests* (delete pr *pending-requests*))
	pr))))

(defun add-pending-request (addr tid)
  (let ((pr (make-pending-request :addr addr :tid tid)))
    (mp:with-process-lock (*pending-requests-lock*)
      (push pr *pending-requests*))
    pr))

(defun udp-receiver (s)
  (loop
    (multiple-value-bind (buf size ip port)
	(socket:receive-from s (expt 2 16))
      (let ((addr (make-addr :ip ip :port port)))
	(format t "udp-receiver: Received ~a bytes from ~a~%" size addr)
	(if* (< size 8)
	   then (format t "bogus packet len.~%")
	   else (multiple-value-bind (action tid)
		    (parse-udp-track-response-header buf)
		  (format t "tid: ~a, action: ~a~%" tid action)
		  (let ((pr (lookup-and-remove-pending-request addr tid)))
		    (if* (null pr)
		       then (format t "No match in pending requests list.~%")
		       else (format t "Opening gate.~%")
			    (setf (pending-request-buf pr) buf)
			    (setf (pending-request-size pr) size)
			    (mp:open-gate (pending-request-gate pr))))))))))

(defun send-message (s addr action payload size deadline)
  (let ((h (ensure-udp-tracker-host addr))
	(buf (make-usb8 (+ size 16)))
	(tid (random32)))

    (dotimes (n size)
      (setf (aref buf (+ 16 n)) (aref payload n)))
    
    (let* ((pr (add-pending-request addr tid))
	   (gate (pending-request-gate pr)))
      
      (format t "send-message: action ~a, len: ~a, tid: ~a~%" action (length buf) tid)
      
      (loop
	(let* ((cid (if* (eq action *action-connect*)
		       then #x41727101980
		       else (ensure-connection-id s h deadline)))
	       (now (get-universal-time))
	       (transmission-timeout (* 15 (expt 2 (udp-tracker-host-retry-count h))))
	       (transmission-deadline (+ now transmission-timeout))
	       (selected-deadline (min transmission-deadline deadline))
	       (selected-transmission-timeout (- selected-deadline now)))
	  (format t "selected transmission timeout: ~a~%" selected-transmission-timeout)
	  (when (<= selected-transmission-timeout 0)
	    ;; Deadline exceeded
	    (assert (eq pr (lookup-and-remove-pending-request addr tid)))
	    (error "timeout"))
	  (populate-udp-track-request-header buf cid action tid)
	  (socket:send-to s buf (length buf)
			  :remote-host (addr-ip addr)
			  :remote-port (addr-port addr))
	  (format t "Message sent.~%")
	  (if* (mp:process-wait-with-timeout "Waiting for response" selected-transmission-timeout #'mp:gate-open-p gate)
	     then ;; Got a response.
		  (setf (udp-tracker-host-retry-count h) 0)
		  (let* ((buf (pending-request-buf pr))
			 (size (pending-request-size pr))
			 (resp-action (get32-net buf 0)))
		    (when (eq resp-action *action-error*)
		      (error "Got error response: ~a" (octets-to-string buf :start 8 :end size :external-format :utf8)))
		    (return (values buf size)))
	     else ;; Timeout
		  (format t "transmission timeout.~%")
		  (setf (udp-tracker-host-retry-count h)
		    (min (1+ (udp-tracker-host-retry-count h)) 8))))))))

(defun request-connection-id (s addr deadline)
  (let* ((resp (send-message s addr *action-connect* nil 0 deadline))
	 (cid (get64-net resp 8)))
    (format t "Got CID ~a~%" cid)
    cid))

(defun ensure-connection-id (s h deadline)
  (let ((cid (udp-tracker-host-cid h))
	(exp (udp-tracker-host-cid-expiration-time h))
	(now (get-universal-time)))
    (if* (and cid exp (< now exp))
       then ;; Good to go
	    (format t "Using still-good cid ~a~%" cid)
	    cid
       else (prog1 
		;; Return value
		(setf (udp-tracker-host-cid h) 
		  (request-connection-id s (udp-tracker-host-addr h) deadline))
	      (setf (udp-tracker-host-cid-expiration-time h) (+ (get-universal-time) 60))))))

(defstruct announce-response 
  interval
  leechers
  seeders
  addrs
  )

(defun decode-udp-announce-response (buf len)
  (let ((pos 8))
    (macrolet ((g32 ()
		 `(prog1 (get32-net buf pos)
		    (incf pos 4)))
	       (g16 ()
		 `(prog1 (get16-net buf pos)
		    (incf pos 2))))
      (let ((r (make-announce-response 
		:interval (g32)
		:leechers (g32)
		:seeders (g32)))
	    addrs)
	(while (< pos len)
	  (push (make-addr :ip (g32) :port (g16)) addrs))
	(setf (announce-response-addrs r) addrs)
	r))))

(defun udp-announce (s addr info-hash &key (downloaded 0) (left 0) (uploaded 0) (event *event-none*) 
					   (key 0))
  (let ((buf (make-usb8 (- 98 16)))
	(pos 0))
    (etypecase info-hash
      (simple-string 
       (dotimes (n 20)
	 (setf (aref buf n) (char-code (schar info-hash n))))))
    (incf pos 20)
    (put160-net buf pos *our-id*)
    (incf pos 20)
    (put64-net buf pos downloaded)
    (incf pos 8)
    (put64-net buf pos left)
    (incf pos 8)
    (put64-net buf pos uploaded)
    (incf pos 8)
    (put32-net buf pos event)
    (incf pos 4)
    (put32-net buf pos 0) ;; IP address
    (incf pos 4)
    (put32-net buf pos key) 
    (incf pos 4)
    (put32-net buf pos #xffffffff)
    (incf pos 4)
    (put16-net buf pos (socket:local-port s))
    (incf pos 2)
    
    (multiple-value-bind (resp resp-len)
	(send-message s addr *action-announce* buf (length buf) (+ (get-universal-time) 30))
      (decode-udp-announce-response resp resp-len))))
