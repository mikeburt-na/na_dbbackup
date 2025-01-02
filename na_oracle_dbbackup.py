#! /usr/bin/env python3

"""
Oracle DB Backup Script

Copyright (c) 2020 NetApp, Inc. All Rights Reserved.
Licensed under the BSD 3-Clause “New” or Revised” License (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
https://opensource.org/licenses/BSD-3-Clause

Print statements can be uncommented if script is to called directly outside of other scripted code.

"""

from netapp_ontap import NetAppRestError
from netapp_ontap.resources import Snapshot,SnapmirrorRelationship,SnapmirrorTransfer,Svm,Volume
from utils import Argument, parse_args, setup_logging, setup_connection
from utils import show_svm, show_volume, get_key_volume, show_snapshot
from datetime import datetime


def list_snapshot(args) -> None:
    """List Snapshots on Selected DB Backup Volume"""
    svm_name = args.cluster
    volume_name = args.volume_name
    vol_uuid = get_key_volume(svm_name, volume_name)
    try:
        #print()
        #print("Oracle DB Backup Snapshot list for:")
        #print("SVM: " + svm_name)
        #print("Volume: " + volume_name)
        #print("======================================================================")
        for snapshot in Snapshot.get_collection(vol_uuid):
            print(svm_name + ":" + volume_name + ":" + snapshot.name)
    except NetAppRestError as error:
        print("Exception caught :" + str(error))


def list_dest_snapshot(args) -> None:
    """List Snapshots on Destination Volume.  If one exists"""
    SourceVolume = args.volume_name
    SourceSVM = args.cluster
    SourcePath = SourceSVM + ':' + SourceVolume
    try:
        for snapmirrorsource in SnapmirrorRelationship.get_collection(fields="source,destination",list_destinations_only=True):
            if snapmirrorsource.source.path == SourcePath:
                snapmirrordestsvm = snapmirrorsource.destination.svm.name
                snapmirrordestpath = snapmirrorsource.destination.path
                snapmirrordestvol = snapmirrordestpath.split(':',1)[1]
                setup_connection(snapmirrordestsvm, args.api_user, args.api_pass)
                vol_uuid = get_key_volume(snapmirrordestsvm, snapmirrordestvol)
                #print()
                #print("Oracle DB Backup Snapshot list for Destination:")
                #print("SVM: " + snapmirrordestsvm)
                #print("Volume: " + snapmirrordestvol)
                #print("======================================================================")
                for snapshot in Snapshot.get_collection(vol_uuid):
                    print(snapmirrordestsvm + ":" + snapmirrordestvol + ":" + snapshot.name)
                break
    except NetAppRestError as error:
        print("Exception caught :" + str(error))


def create_snapshot(args) -> None:
    """Create snapshot on Source Volume"""
    svm_name = args.cluster
    volume_name = args.volume_name
    vol_uuid = get_key_volume(svm_name, volume_name)
    snapshot_name = args.snapshot

    snapshot = Snapshot.from_dict(
        {
            'name': snapshot_name,
            'snapmirror_label': 'Vault',
            'volume':{'name': volume_name,'uuid': vol_uuid}
        }
    )

    try:
        print()
        print("Oracle DB Backup Snapshot Creation Request Successful:")
        print("Snapshot: " + snapshot_name)
        print("SVM: " + svm_name)
        print("Volume: " + volume_name)
        print("======================================================================")
        if snapshot.post(poll=True):
            print("Snapshot  %s created Successfully" % snapshot.name)
    except NetAppRestError as error:
        print("Exception caught :" + str(error))


def delete_snapshot(args) -> None:
    """Delete Snapshot"""
    svm_name = args.cluster
    volume_name = args.volume_name
    vol_uuid = get_key_volume(svm_name, volume_name)
    snapshot_name = args.snapshot

    try:
        snapshot = Snapshot.find(vol_uuid, name=snapshot_name)
    except NetAppRestError as error:
        print("Exception caught :" + str(error))

    try:
        print()
        print("Oracle DB Backup Snapshot Deletion Request Successful:")
        print("Snapshot: " + snapshot_name)
        print("SVM: " + svm_name)
        print("Volume: " + volume_name)
        print("======================================================================")
        if snapshot.delete(poll=True):
            print(
                "Snapshot  %s has been deleted Successfully." %
                snapshot.name)
    except NetAppRestError as error:
        print("Exception caught :" + str(error))


def update_snapmirror(args) -> None:
    """Connect to Source SVM and Retrieves Destination SVM and Mirror Volume."""
    """If mirror exists, check state, if 'snapmirrored', then POST update, else Skip Update"""
    SourceVolume = args.volume_name
    SourceSVM = args.cluster
    SourcePath = SourceSVM + ':' + SourceVolume
    try:
        for snapmirrorsource in SnapmirrorRelationship.get_collection(fields="source,destination",list_destinations_only=True):
            if snapmirrorsource.source.path == SourcePath:
                snapmirrordestsvm = snapmirrorsource.destination.svm.name
                setup_connection(snapmirrordestsvm, args.api_user, args.api_pass)
                for snapmirrordest in SnapmirrorRelationship.get_collection(fields="source"):
                    if snapmirrordest.source.path == SourcePath:
                        snapmirrorDetail = SnapmirrorRelationship(uuid=snapmirrordest.uuid)
                        snapmirrorDetail.get()
                        snapmirrorUpdate = SnapmirrorTransfer(snapmirrorDetail.uuid)
                        if snapmirrorDetail.state == 'snapmirrored':
                            snapmirrorUpdate.post()
                            snapmirrorUpdate.get()
                            print()
                            print("Oracle DB Backup Snapmirror Update Successfully Initiated")
                            print("Source Path: " + snapmirrorDetail.source.path + "---->Destination Path: " + snapmirrorDetail.destination.path)
                            print("Previous State: " + snapmirrorDetail.state + "---->Current State: " + snapmirrorUpdate.state)
                            print("======================================================================")
                        else:
                            print('Mirror is already Transferring or Unhealthy.  Mirror State: ' + snapmirrorDetail.state)
                        break
                break
    except NetAppRestError as error:
        print("Exception caught :" + str(error))


def show_dest_svm(args) -> None:
    """Connect to Source SVM and Retrieves Destination SVM"""
    #SourceVolume = args.volume_name
    #SourceSVM = args.cluster
    try:
        for snapmirrorsource in SnapmirrorRelationship.get_collection(fields="source,destination",list_destinations_only=True):
                snapmirrordestsvm = snapmirrorsource.destination.svm.name
                print(snapmirrordestsvm)
                break
    except NetAppRestError as error:
        print("Exception caught :" + str(error))


def create_clone(args) -> None:
    """Create clone of Volume"""
    svm_name = args.cluster
    volume_name = args.volume_name
    vol_uuid = get_key_volume(svm_name, volume_name)
    snapshot_name = args.snapshot
    clone_name_manual = args.clone_name
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y_%H%M%S")
    clone_name_auto = snapshot_name + '_CLONE_' + dt_string

    if clone_name_manual:
        clone_name = clone_name_manual
    else: 
        clone_name = clone_name_auto

    snapshotclone = Volume.from_dict(
        {
            'name':clone_name,
            'clone':{"parent_volume": {"name": volume_name},"parent_snapshot": {"name": snapshot_name}, "is_flexclone": "true"},
            'svm':{"name": svm_name},
            'nas':{"path": "/" + clone_name}
        }
    )

    try:
        print()
        print("Oracle DB Backup Snapshot Clone Creation Request Successful:")
        print("Snapshot: " + snapshot_name)
        print("SVM: " + svm_name)
        print("Parent Volume: " + volume_name)
        print("Clone: " + snapshotclone.name)
        print("======================================================================")
        if snapshotclone.post(hydrate=True):
            print("Volume Clone %s created Successfully" % snapshotclone.name)
    except NetAppRestError as error:
        print("Exception caught :" + str(error))


def list_clone(args) -> None:
    """List Volume Clones"""
    svm_name = args.cluster
    volume_name = args.volume_name
    vol_uuid = get_key_volume(svm_name, volume_name)
    
    try:
        #print()
        #print("Oracle DB Backup Clone list for:")
        #print("SVM: " + svm_name)
        #print("======================================================================")
        for volume in Volume.get_collection(fields="clone"):
            if volume.clone.is_flexclone == True:
                if volume.clone.parent_volume.name == volume_name:
                    print(volume.name)
    except NetAppRestError as error:
        print("Exception caught :" + str(error))


def delete_clone(args) -> None:
    """Delete Clone Volume"""
    svm_name = args.cluster
    volume_name = args.volume_name
    vol_uuid = get_key_volume(svm_name, volume_name)
    snapshot_name = args.snapshot

    try:
        volume = Volume.find(uuid = vol_uuid)
    except NetAppRestError as error:
        print("Exception caught :" + str(error))

    try:
        print()
        print("Oracle DB Backup Clone Volume Deletion Request Successful:")
        print("SVM: " + svm_name)
        print("Volume: " + volume_name)
        print("======================================================================")
        volume_clone = Volume.find(uuid = vol_uuid)
        #print(volume_clone)
        if volume_clone.clone.is_flexclone == True:
            if volume.delete(poll=True):
                print(
                    "Clone Volume  %s has been deleted Successfully." %
                    volume.name)
        else:
            print("Failed: Selected Volume is not a Clone")
    except NetAppRestError as error:
        print("Exception caught :" + str(error))


def snapshot_ops(args) -> None:
    """Snapshot Operation"""
    #print("Oracle DB Backup - NetApp Snapshot Operations")
    #print("======================================================================")
    #print()
    snapshotbool = args.snapshot_action
    if snapshotbool == 'list':
        list_snapshot(args)
    if snapshotbool == 'create':
        create_snapshot(args)
    if snapshotbool == 'delete':
        delete_snapshot(args)
    if snapshotbool == 'smupdate':
        update_snapmirror(args)
    if snapshotbool == 'show_dest_svm':
        show_dest_svm(args)
    if snapshotbool == 'list_dest':
        list_dest_snapshot(args)
    if snapshotbool == 'create_clone':
        create_clone(args)
    if snapshotbool == 'list_clone':
        list_clone(args)
    if snapshotbool == 'delete_clone':
        delete_clone(args)


def main() -> None:
    """Main function"""

    arguments = [
        Argument("-c", "--cluster", "API server IP:port details")]
    args = parse_args(
        "Oracle DB Backup - NetApp Snapshot Operations",
        arguments,
    )
    setup_logging()
    setup_connection(args.cluster, args.api_user, args.api_pass)

    snapshot_ops(args)


if __name__ == "__main__":
    main()
