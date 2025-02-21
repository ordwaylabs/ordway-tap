pipeline {
    agent {
        label 'slave1'
    }

    environment {
        suffix = "${params.branch}-${currentBuild.startTimeInMillis}"
    }

    stages {
        stage('Update Workspace') {
    steps {
        script {
            echo "Fetching latest changes from Git repository..."
            sh """
                cd /data/workspace/singer-tap_dev
                git fetch --all
                git reset --hard origin/${params.branch}
                git clean -fd
                git pull origin ${params.branch}
            """
        }
    }
}
        stage('Build') {
            steps {
                echo "Building branch ${params.branch}"
            }
        }

        stage('Build Image') {
            steps {
                script {
                    sh """
                    hostname
                    docker stop \$(docker ps -a --filter "name=tap" --format "{{.ID}}") || true
                    docker rm \$(docker ps -aq --filter "name=tap") || true
                    docker image prune -a --force || true
                    docker build -f /data/workspace/singer-tap_dev/Dockerfile -t tap:${suffix} /data/workspace/singer-tap_dev
                    """
                }
            }
        }

        stage('Creating Docker Container') {
            steps {
                script {
                    sh """
                    docker run -itd --cpus="0.5" --memory="0.5g" \
                    -v /data/workspace/singer-tap_dev:/app --name tap tap:${suffix}
                    """
                }
            }
        }

        stage('Execute Tap Commands') {
            steps {
                script {
                    sh """
                    docker exec tap sh -c 'python3 -c "import os, json; print(json.dumps(dict(os.environ), indent=2))" > /app/config.json'
                    docker exec -it tap  sh -c "export COMPANY='$COMPANY' export USER_EMAIL='$USER_EMAIL' export USER_TOKEN='$USER_TOKEN' export API_URL= '$API_URL' export API_KEY= 'API_KEY' export START_DATE = '$START_DATE'"
                    docker exec tap cat /app/config.json
                    docker exec tap sh -c 'tap-ordway -c /app/config.json --catalog /app/catalog.json | target-stitch --config /app/stitch_config.json'
                    """
                }
            }
        }

       
    }

   
}
