"""Working with long Auki sessions in segments, from notebooks or scripts; no runner uses these.

- segment: split one session into overlapping sub-sessions.
- stitch_qr: put refined segments into one frame through the QR markers they share.
- stitch_trajectory: put refined segments back into the session's own trajectory frame, and
  write global outputs from them.
- rig_merge: merge reconstructions without dissolving their multi-sensor rigs.

Import them from the repository root with PYTHONPATH=. (the package and these modules).
"""
