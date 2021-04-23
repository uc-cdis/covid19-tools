if ! bash "/nb-etl/nb-etl-run.sh"; then
    echo "JOB FAILED: nb-etl-job"
    if [[ -n "$slackWebHook" && "$slackWebHook" != "None" ]]; then
        echo "Posting failed message to slack.."
        payload="{\"attachments\": [{\"fallback\": \"JOB FAILED: nb-etl job on ${gen3Env}\",\"color\": \"#ff0000\",\"title\": \"JOB FAILED: nb-etl job on ${gen3Env}\",\"text\": \"Pod name: ${HOSTNAME}\",\"ts\": "$(date +%s)"}]}"
        echo "${payload}"
        curl -X POST --data-urlencode "payload=${payload}" "${slackWebHook}"
    fi
else
    echo "JOB SUCCESS: nb-etl-job"
    if [[ -n "$slackWebHook" && "$slackWebHook" != "None" ]]; then
        echo "Posting success message to slack.."
        payload="{\"attachments\": [{\"fallback\": \"JOB SUCCESS: nb-etl job on ${gen3Env}\",\"color\": \"#2EB67D\",\"title\": \"JOB SUCCESS: nb-etl job on ${gen3Env}\",\"text\": \"Pod name: ${HOSTNAME}\",\"ts\": \"$(date +%s)\"}]}"
        curl -X POST --data-urlencode "payload=${payload}" "${slackWebHook}"
    fi
fi
