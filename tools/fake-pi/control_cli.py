#!/usr/bin/env python3
"""control_cli.py — the GUI's side of the contract, as a CLI.

Calls SkycamControlService on the ingest service's *control* listener. That
listener binds to loopback and takes a separate operator token, because anything
that can call it can re-point a live camera.

  OBS_CONTROL_TARGET   default 127.0.0.1:9091
  INGEST_OPERATOR_TOKEN

  python control_cli.py list
  python control_cli.py settings fake-skycam --gain 300 --exposure-ms 2000
  python control_cli.py settings fake-skycam --preview-gamma 0.5
  python control_cli.py command fake-skycam capture-now
  python control_cli.py command fake-skycam abort-exposure
"""
import argparse
import os
import sys
import uuid

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen"))
from skycam.v1 import skycam_pb2 as pb  # noqa: E402
from skycam.v1 import skycam_pb2_grpc as rpc  # noqa: E402

TARGET = os.environ.get("OBS_CONTROL_TARGET", "127.0.0.1:9091")
TOKEN = os.environ.get("INGEST_OPERATOR_TOKEN", "")


def main():
    p = argparse.ArgumentParser(description="operator CLI for the ingest control API")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="devices holding an open DeviceSession")

    s = sub.add_parser("settings", help="store settings and push them if connected")
    s.add_argument("device_id")
    s.add_argument("--exposure-ms", type=float)
    s.add_argument("--gain", type=int)
    s.add_argument("--preview-gamma", type=float)
    s.add_argument("--preview-contrast", type=float)
    s.add_argument("--preview-brightness", type=float)

    c = sub.add_parser("command", help="send a command to a connected device")
    c.add_argument("device_id")
    c.add_argument("kind", choices=["capture-now", "abort-exposure"])
    c.add_argument("--ack-timeout-ms", type=int, default=5000)

    args = p.parse_args()
    if not TOKEN:
        print("INGEST_OPERATOR_TOKEN is not set", file=sys.stderr)
        return 1

    md = (("authorization", f"Bearer {TOKEN}"),)
    with grpc.insecure_channel(TARGET) as ch:
        stub = rpc.SkycamControlServiceStub(ch)
        try:
            if args.cmd == "list":
                resp = stub.ListConnectedDevices(pb.ListConnectedDevicesRequest(), metadata=md)
                if not resp.devices:
                    print("no devices connected")
                for d in resp.devices:
                    print(f"{d.device_id}  connected_at={d.connected_at.ToDatetime()}Z  "
                          f"last_message={d.last_message_at.ToDatetime()}Z")

            elif args.cmd == "settings":
                st = pb.DeviceSettings(device_id=args.device_id)
                for field, value in (("exposure_ms", args.exposure_ms), ("gain", args.gain),
                                     ("preview_gamma", args.preview_gamma),
                                     ("preview_contrast", args.preview_contrast),
                                     ("preview_brightness", args.preview_brightness)):
                    if value is not None:
                        setattr(st, field, value)
                resp = stub.UpdateDeviceSettings(
                    pb.UpdateDeviceSettingsRequest(settings=st), metadata=md)
                # delivered=False is not a failure: the row is stored either way
                # and the device reads it on its next connect.
                print(f"stored; pushed_now={resp.delivered}")

            elif args.cmd == "command":
                cmd = pb.Command(command_id=str(uuid.uuid4()))
                if args.kind == "capture-now":
                    cmd.capture_now.SetInParent()
                else:
                    cmd.abort_exposure.SetInParent()
                resp = stub.SendCommand(pb.SendCommandRequest(
                    device_id=args.device_id, command=cmd,
                    ack_timeout_ms=args.ack_timeout_ms), metadata=md)
                print(f"command {resp.command_id[:8]} acknowledged={resp.acknowledged}"
                      + (f" error={resp.error}" if resp.error else ""))
        except grpc.RpcError as e:
            print(f"{e.code().name}: {e.details()}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
