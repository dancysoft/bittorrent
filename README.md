Allegro CL BitTorrent client

To load do

    :ld load.cl

Then:

  (bdecode-file "filename.torrent")

which will return a 'dict' struct. Use (dict-get "announce" dict) to
get the primary tracker. Use (dict-get "announce-list" dict) to get a
list of other trackers (which may include the primary tracker).
