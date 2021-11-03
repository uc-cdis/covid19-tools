if ! bash "$HOME/gbm/gbm-run.sh"; then
    echo "JOB FAILED: generative Bayes model"
    if [[ -n "$slackWebHook" && "$slackWebHook" != "None" ]]; then
        echo "Posting failed message to slack.."
        payload="{\"attachments\": [{\"fallback\": \"JOB FAILED: generative Bayes model job on ${gen3Env}\",\"color\": \"#ff0000\",\"title\": \"JOB FAILED: generative Bayes model job on ${gen3Env}\",\"text\": \"Pod name: ${HOSTNAME}\",\"ts\": "$(date +%s)"}]}"
        echo "${payload}"
        curl -X POST --data-urlencode "payload=${payload}" "${slackWebHook}"
    fi
else
    echo "JOB SUCCESS: generative Bayes model"
    if [[ -n "$slackWebHook" && "$slackWebHook" != "None" ]]; then
        echo "Posting success message to slack.."
        payload="{\"attachments\": [{\"fallback\": \"JOB SUCCESS: generative Bayes model job on ${gen3Env}\",\"color\": \"#2EB67D\",\"title\": \"JOB SUCCESS: generative Bayes model job on ${gen3Env}\",\"text\": \"Pod name: ${HOSTNAME}\",\"ts\": \"$(date +%s)\"}]}"
        curl -X POST --data-urlencode "payload=${payload}" "${slackWebHook}"
    fi
fi
