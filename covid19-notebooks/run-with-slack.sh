if ! bash "/nb-etl/nb-etl-run.sh"; then
    echo "JOBFAIL: nb-etl-job"
    if [[ -n "$slackWebHook" && "$slackWebHook" != "None" ]]; then
        echo "Posting failed message to slack.."
        payload="{\"attachments\": [{\"fallback\": \"JOBFAIL: nb-etl job on ${gen3Env}\",\"color\": \"#ff0000\",\"pretext\": \"JOBFAIL: nb-etl job on ${gen3Env}\",\"author_name\": \"Pod name: ${HOSTNAME}\",\"title\": \"NB-ETL JOB FAILED\",\"text\": \"JOBFAIL: nb-etl job on ${gen3Env}\",\"ts\": "$(date +%s)"}]}"
        echo "${payload}"
        curl -X POST --data-urlencode "payload=${payload}" "${slackWebHook}"
    fi
else
    echo "JOBSUCCESS: nb-etl-job"
    if [[ -n "$slackWebHook" && "$slackWebHook" != "None" ]]; then
        echo "Posting success message to slack.."
        payload="{\"attachments\": [{\"fallback\": \"JOBSUCCESS: nb-etl job on ${gen3Env}\",\"color\": \"#2EB67D\",\"pretext\": \"JOBFAIL: nb-etl job on ${gen3Env}\",\"author_name\": \"Pod name: ${HOSTNAME}\",\"title\": \"NB-ETL JOB SUCCEDED :tada:\",\"text\": \"JOBSUCCESS: nb-etl job on ${gen3Env}\",\"ts\": \"$(date +%s)\"}]}"
        curl -X POST --data-urlencode "payload=${payload}" "${slackWebHook}"
    fi
fi
