import datetime
from http.client import HTTPException
import frappe
from fastapi import Depends, APIRouter, Request, Response
from fast_frappe.ctrl import init_frappe, destroy_frappe
import time
from fast_frappe.socketio import sio
default_space_id = 'default'
router = APIRouter()


@router.post("/api/v1/push")
async def handlePush(req: Request, res: Response):
    init_frappe()
    push = await req.json()
    # t0 = datetime.now()
    t0 = time.monotonic()
    try:
        # iterate each mutation in the push
        for mutation in push['mutations']:
            t1 = time.monotonic()
            try:
                processMutation(psg=frappe, clientID=push['clientID'], spaceID=default_space_id, mutation=mutation)
            except Exception as e:
                processMutation(psg=frappe, clientID=push['clientID'], spaceID=default_space_id, mutation=mutation, error=e)
            print(f'Processed mutation in {datetime.now() - t1}')
        sio.emit("default", "poke", namespace="/")
        print("Sent ws response, poke")
        return {}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        print(f"Processed push in {time.monotonic() - t0:.2f} seconds")
    destroy_frappe()

def processMutation(psg, clientID, spaceID, mutation, error):
    """
    Table space only has 2 col (key, version)
    Process a mutation from the client
    """
    # _dict = frappe.db.sql(f"""select version from tabSpace where key = '{spaceID}' for update""").as_dict()
    prev_version = frappe.db.get_value('RepSpace', filters={"key": spaceID}, fieldname='version')
    nextVersion = prev_version + 1
    lastMutationID = getLatestMutationID(psg, clientID=clientID, required=False)
    nextMutationID = lastMutationID + 1

    if mutation['id'] < nextMutationID:
        raise Exception("Mutation ID has already been processed - skipping")

    if mutation['id'] > nextMutationID:
        raise Exception("Mutation ID is from the future - aborting")

    if (error is None):
        print(f'Processing mutation: {mutation}')
        if mutation['name'] == 'createMessage':
            createMessage(mutation['type'], mutation['args'], spaceID, nextVersion)
        else:
            raise Exception(f"Unknown mutation {mutation['name']}")
    else:
        print(f'Error processing mutation: {mutation}')

    setLatestMutationID(psg, clientID, nextMutationID)
    # update version for space
#     await t.none('update space set version = $1 where key = $2', [
#     nextVersion,
#     spaceID,
#   ]);
    # frappe.db.set_value("Space", spaceID, "version", nextVersion)


def getLatestMutationID(psg, clientID, required):
    """Not sure I need the required here

    Args:
        psg (_type_): _description_
        clientID (_type_): _description_
        required (_type_): _description_

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """
    # clientRow = frappe.db.get_value("Replicache Client", client_id=clientID, latest_mutation_id=required)
    clientRow = frappe.db.get_doc("RepClient", fitlers={'client_id': 'clientID'})
    if not clientRow:
        if required:
            raise Exception(f"Client not found {clientID}")
        return 0
    else:
        # TODO parse in correct format
        return clientRow.latest_mutation_id


def setLatestMutationID(psg, clientID, mutationID):
    """_summary_

    Args:
        psg (_type_): _description_
        clientID (_type_): client id to mutate mutationID column in the table Replicache Client
        mutationID (_type_): new mutatation id
    """
    # TODO: direct copy of the javascript which is incorrect, this need to check if there is any row with the clientID, mutationID and update them
    # result = frappe.db.get_value("Replicache Client", client_id=clientID, latest_mutation_id=mutationID)
    result = frappe.get_doc("RepClient", fitlers={'client_id': 'clientID'})
    if result:
        result.set("latest_mutation_id", mutationID)
        result.save()
    else:
        result = frappe.new_doc({
            "doctype": "Replicache Client",
            "client_id": clientID,
            "latest_mutation_id": mutationID
        })
        result.save()
        # frappe.db.insert({
        #     "doctype": "Replicache Client",
        #     "client_id": clientID,
        #     "latest_mutation_id": mutationID
        # })


def createMessage(t, _dict, spaceID, version):
    """_summary_

    Args:
        t (_type_): _description_
        _dict (_type_): _description_
        spaceID (_type_): _description_
        version (_type_): _description_
    """
    # using frappe query builder to insert into the database
    _id, _from, content, order = _dict
    new_mess = frappe.new_doc({
        "doctype": "Replicache Message",
        "type": t,
        "id": _id,
        "from": _from,
        "content": content,
        "order": order,
        "space_id": spaceID,
        "version": version
    })
    new_mess.save()

    # frappe.db.insert({
    #     "doctype": "Replicache Message",
    #     "type": t,
    #     "id": _id,
    #     "from": _from,
    #     "content": content,
    #     "order": order,
    #     "space_id": spaceID,
    #     "version": version
    # })


# def sendPoke(sio):
#     '''Send a poke to the client to trigger a pull'''
#     sio.emit('poke', channel='replicache')
