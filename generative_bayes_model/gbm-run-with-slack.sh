if ! bash "$HOME/gbm/gbm-run.sh"; then
    echo "JOB FAILED: gbm-run.sh"
    if [[ -n "$slackWebHook" && "$slackWebHook" != "None" ]]; then
        echo "Posting failed message to slack.."
        payload="{\"attachments\": [{\"fallback\": \"JOB FAILED: gbm-run.sh job on ${gen3Env}\",\"color\": \"#ff0000\",\"title\": \"JOB FAILED: gbm-run.sh job on ${gen3Env}\",\"text\": \"Pod name: ${HOSTNAME}\",\"ts\": "$(date +%s)"}]}"
        echo "${payload}"
        curl -X POST --data-urlencode "payload=${payload}" "${slackWebHook}"
    fi
else
    echo "JOB SUCCESS: gbm-run.sh"
    if [[ -n "$slackWebHook" && "$slackWebHook" != "None" ]]; then
        echo "Posting success message to slack.."
        payload="{\"attachments\": [{\"fallback\": \"JOB SUCCESS: gbm-run.sh job on ${gen3Env}\",\"color\": \"#2EB67D\",\"title\": \"JOB SUCCESS: gbm-run.sh job on ${gen3Env}\",\"text\": \"Pod name: ${HOSTNAME}\",\"ts\": \"$(date +%s)\"}]}"
        curl -X POST --data-urlencode "payload=${payload}" "${slackWebHook}"
    fi
fi
