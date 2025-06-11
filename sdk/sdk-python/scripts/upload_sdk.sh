# We re-attempt the publish only in case of facing either of the errors listed below:
# HTTPError: 400 Bad Request: The package is missing some required fields in its metadata, such as name, version, author, etc.
# HTTPError: 504 Gateway Time-out: The server took too long to respond to the request.

upload_cmd="python3 -m twine upload --verbose --repository-url https://us-python.pkg.dev/bios-eng/python-sdk/ $ROOT/sdk-python/target/*"
reattempt_code_list=("400 Bad Request" "504 Gateway Time-out")

for attempt_count in {1..3}; do
    reattempt=0
    error_message=$($upload_cmd 2>&1 >/dev/null)
    return_code=$?
    echo "Python SDK upload attempt $attempt_count returned code: $return_code"
    if [ $return_code != 0 ]; then
        echo && echo "Error while publishing python package: $error_message"
        for error_code in "${reattempt_code_list[@]}"; do
            if [[ $error_message == *"$error_code"* ]]; then
                echo && echo "Reattempting publish..."
                reattempt=1
                break
            fi
        done
    fi
    if [ $reattempt == 0 ]; then
        break
    fi
done
