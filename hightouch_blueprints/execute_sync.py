import argparse
import sys
import requests
import shipyard_utils as shipyard
import errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--sync-id', dest='sync_id', required=True)
    args = parser.parse_args()
    return args


def execute_sync(sync_id, access_token):
    """
    Executes/starts a Hightouch Sync
    see: https://hightouch.io/docs/api-reference/#operation/TriggerRun
    """
    
    sync_api = f"https://api.hightouch.io/api/v1/syncs/{sync_id}/trigger"
    api_headers = {
        'authorization': f"Bearer {access_token}",
        'Content-Type': 'application/json'
    }
    payload = {
        "fullResync": "false"
    }
    try:
        sync_trigger_response = requests.post(sync_api,
                                              json=payload,
                                              headers=api_headers)
        
        sync_status_code = sync_trigger_response.status_code
        sync_trigger_json = sync_trigger_response.json()
        # check if successful, if not return error message
        if sync_status_code == requests.codes.ok:
            return sync_trigger_json
            
        elif sync_status_code == 400: # Bad request
            print("Sync request failed due to Bad Request Error.")
            sys.exit(errors.EXIT_CODE_BAD_REQUEST)
            
        elif sync_status_code == 401: # Incorrect credentials 
            print("Incorrect credentials, check your access token")
            sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)
            
        elif sync_status_code == 422: # Invalid content
            print(f"Validation failed: {sync_trigger_json['details']}")
            sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)
        
        else:
            print("Unknown Error when sending request")
            sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
        
    except Exception as e:
        print(f"Sync trigger request failed due to: {e}")
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)

    


def main():
    args = get_args()
    access_token = args.access_token
    sync_id = args.sync_id
    
    # execute trigger sync
    trigger_sync = execute_sync(sync_id, access_token)
    sync_run_id = trigger_sync['id']
    
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'hightouch')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
    
    # save sync run id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths, 
                                'sync_run_id', sync_run_id)

if __name__ == "__main__":
    main()
