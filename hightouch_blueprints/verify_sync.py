import argparse
import sys
import requests
import shipyard_utils as shipyard
try:
    import errors
except BaseException:
    from . import errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--sync-id', dest='sync_id', required=True)
    parser.add_argument('--sync-run-id', dest='sync_run_id', required=False)
    args = parser.parse_args()
    return args


def get_sync_status(sync_id, sync_run_id, access_token):
    """
    Gets the sync status from the Hightouch API
    see: https://hightouch.io/docs/api-reference/#operation/ListSyncRuns
    """
    sync_api = f"https://api.hightouch.io/api/v1/syncs/{sync_id}/runs"
    api_headers = {
        'authorization': f"Bearer {access_token}",
        'Content-Type': 'application/json'
    }
    params = {
        "runId": sync_run_id
    }
    try:
        sync_run_response = requests.get(sync_api,
                                         params=params,
                                         headers=api_headers)

        sync_status_code = sync_run_response.status_code
        # check if successful, if not return error message
        if sync_status_code == requests.codes.ok:
            sync_run_json = sync_run_response.json()

            # Handles an error where the response is successful, but a blank list
            # is returned when runID provide returns no matches.
            if len(sync_run_json['data']) == 0:
                print(f"Sync request Failed. Invalid Sync ID: {sync_id}")
                sys.exit(errors.EXIT_CODE_SYNC_INVALID_ID)
            else:
                return sync_run_json['data'][0]

        elif sync_status_code == 400:  # Bad request
            print("Sync status request failed due to Bad Request Error.")
            sys.exit(errors.EXIT_CODE_BAD_REQUEST)

        elif sync_status_code == 401:  # Incorrect credentials
            print("Incorrect credentials, check your access token")
            sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)

        elif sync_status_code == 422:  # Invalid status query
            sync_run_json = sync_run_response.json()
            print(
                f"Check status Validation failed: {sync_run_json['details']}")
            sys.exit(errors.EXIT_CODE_BAD_REQUEST)

        else:
            print("Unknown Error when sending request")
            sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)

    except Exception as e:
        # Handle an error where a blank list is returned when the
        print(
            f"Failed to grab the sync status for Sync {sync_id}, Sync Run {sync_run_id} due to: {e}")
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)


def determine_run_status(sync_run_data):
    """
    Analyses sync run data to determine status and print sync run information

    Returns:
        status_code: Exit Status code detailing sync status
    """
    run_id = sync_run_data['id']
    status = sync_run_data['status']
    if status == "success":
        print(
            f"Sync run {run_id} completed successfully. ",
            f"Completed at: {sync_run_data['finishedAt']}"
        )
        status_code = errors.EXIT_CODE_FINAL_STATUS_SUCCESS

    elif sync_run_data['finishedAt'] is None:
        print(
            f"Sync run {run_id} still Running. ",
            f"Current records processed: {sync_run_data['records_processed']}"
        )
        status_code = errors.EXIT_CODE_FINAL_STATUS_RUNNING

    elif status == "failed":
        error_info = sync_run_data['error']
        print(f"Sync run {run_id} failed. {error_info}")
        status_code = errors.EXIT_CODE_FINAL_STATUS_FAILED

    else:
        print("Unknown Sync status: {status}")
        status_code = errors.EXIT_CODE_UNKNOWN_ERROR

    return status_code


def main():
    args = get_args()
    access_token = args.access_token
    sync_id = args.sync_id
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'hightouch')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # get sync run id variable from user or pickle file if not inputted
    if args.sync_run_id:
        sync_run_id = args.sync_run_id
    else:
        sync_run_id = shipyard.logs.read_pickle_file(
            artifact_subfolder_paths, 'sync_run_id')
    # run check sync status
    sync_run_data = get_sync_status(sync_id, sync_run_id, access_token)
    # save sync run data as json file
    sync_run_data_file_name = shipyard.files.combine_folder_and_file_name(
        artifact_subfolder_paths['responses'],
        f'sync_run_{sync_run_id}_response.json')
    shipyard.files.write_json_to_file(sync_run_data, sync_run_data_file_name)
    # return final status
    exit_code_status = determine_run_status(sync_run_data)
    sys.exit(exit_code_status)


if __name__ == "__main__":
    main()
