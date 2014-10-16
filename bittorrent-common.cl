(in-package :user)

(deftype usb8 () '(unsigned-byte 8))
(deftype ausb8 () '(simple-array usb8 (*)))

(defmacro make-usb8 (size &rest rest)
  `(make-array ,size :element-type 'usb8 ,@rest))
