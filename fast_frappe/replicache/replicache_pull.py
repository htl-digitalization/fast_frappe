import datetime
import time
from fast_frappe.replicache.replicache_push import getLatestMutationID
import frappe
from fastapi import FastAPI, Depends, Request, Response, HTTPException, Depends, APIRouter 
from fast_frappe.socketio import sio


router = APIRouter()


@router.post('/api/v1/pull')
def handle_pull(req: Request, res: Response):
    defaultSpaceID = 'default'
    pull = req.body
    print(f'Processing Pull {pull}')
    t0 = time.monotonic()
    try:
        # await processPull(psg, pull)
        # processPull(pull)
        version = frappe.get_doc('RepSpace', filters={"key": defaultSpaceID}).version
        # Get lmid for requesting client.
        isExistingClient = pull.lastMutationID > 0
        last_mutation_id = getLatestMutationID(frappe, client=pull.clientID, required=isExistingClient)

        # get changed domain objects since req version
        fromVersion = getattr(req, 'cookies', 0)
        doctype = frappe.qb.DocType("RepMessage")
        # TODO: return as list
        changed = frappe.qb.from_(doctype).select('id', 'sender', 'content', 'order', 'deleted').where(doctype.version > fromVersion).run()
        # changed = frappe._qb_()('RepMessage', filters={"version": fromVersion})

        patch = []
        for row in changed:
            if row.deleted:
                if fromVersion > 0:
                    patch.append({
                        'op': 'del',
                        'key': f'message/{row.id}'
                    })
            else:
                patch.append({
                    'op': 'put',
                    'key': f'message/{row.id}',
                    'value': {
                        'from': row.sender,
                        'content': row.content,
                        'order': int(row.order)
                    }
                })
        res.json({
            "lastMutationID": last_mutation_id,
            "cookie": version,
            "patch": patch,
        })
        res.close()
    except Exception as e:
        print(e)
        # res.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        # res.body = str(e).encode()
    finally:
        print(f"Processed pull in {time.time() - t0}")
    return 'test'


# @request.api("GET", "/v1/api/replicache_pull")
# @request.api("GET", '/v1/api/test2')
# def handle_pull(*args, **kwargs):
#     '''Handle the pull request from the client'''
#     return args, kwargs
