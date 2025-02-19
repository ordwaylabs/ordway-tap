pipeline {
    agent { label 'slave1' }

    environment {
        suffix = "${params.Environment}-${params.Runner}-${currentBuild.startTimeInMillis}"
    }

    stages {
        stage('Update Workspace') {
            steps {
                script {
                    echo "Fetching latest changes from Git repository..."
                    sh """
                        cd /data/workspace/singer-tap_dev
                        git reset --hard HEAD
                        git pull origin ${params.Branch}
                    """
                }
            }
        }

        stage('Build') {
            steps {
                echo "Building branch ${params.Branch}"
            }
        }

        stage('Build Image') {
            steps {
                script {
                    def imageExists = sh(
                        script: "docker images -q tap:v1",
                        returnStdout: true
                    ).trim()

                    if (imageExists != "") {
                        echo "Image tap:v1 exists locally. Skipping build."
                    } else {
                        echo "Image not found. Building..."
                        sh "docker build -f /data/workspace/singer-tap_dev/Dockerfile -t tap:v1 ."
                    }
                }
            }
        }

        stage('Create Docker Container') {
            steps {
                sh """
                docker run -itd --rm --cpus="0.5" --memory="0.5g" \
                    -v /data/workspace/singer-tap_dev:/app \
                    --name tap tap:v1
                """
            }
        }

        stage('Execute Singer Tap Commands') {
            steps {
                script {
                    sh """
                    docker exec tap sh -c 'echo "{ 
                        \\"company\\": \\"$company\\",
                        \\"api_key\\": \\"$api_key\\",
                        \\"user_email\\": \\"$user_email\\",
                        \\"user_token\\": \\"$user_token\\",
                        \\"start_date\\": \\"$start_date\\" 
                    }" > /app/config.json'

                    docker exec tap cat /app/config.json
                    docker exec tap tap-ordway -c /app/config.json --catalog /app/catalog.json \
                        | /usr/local/bin/target-stitch --config /app/stitch_config.json
                    """
                }
            }
        }

        stage('Clean Up Docker Containers') {
            steps {
                sh """
                docker stop $(docker ps -a | grep "tap" | awk '{print $1}') || true
                docker rm $(docker ps -aq) || true
                docker image prune -a --force || true
                """
            }
        }
    }

    post {
        failure {
            sh """
            docker stop $(docker ps -a | grep tap | awk '{print $1}') || true
            docker rm $(docker ps -aq) || true
            """
        }
    }
}
