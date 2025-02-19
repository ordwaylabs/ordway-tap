
pipeline {
    agent {
        label 'slave1'
    }

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

        stage('Slack Notification'){

            steps {

                slackSend channel: '#ops',
                          color: '#0000FF',
                    message: "Build Started:\n Job Name: ${env.JOB_NAME}\n Build Number: ${env.BUILD_NUMBER}\n Parameter Passed: ${params.Environment}\n More info at: (<${env.BUILD_URL}|Open>) "
            }
        }
        stage('Build Image') {
              steps {
                 script {
                 def imageExists = sh(
                        script: "docker images -q mvn:v1",
                 returnStdout: true
                  ).trim()

                 if (imageExists != "") {
                   echo "Image mvn:v1 exists locally. Skipping build."
                   } else {
                      echo "Image not found. Building..."
                      sh "docker build -f /data/workspace/singer-tap_dev/Dockerfile -t tap:v1 ."
                  }
               }
             }
    post {
        success {
            slackSend channel: '#ops',
            color: '#00FF00',
            message: "✅ Docker image build succeeded!"
        }
        failure {
            slackSend channel: '#ops',
            color: '#FF0000',
            message: "❌ Docker image build failed!"
        }
    }
}

        stage ('creating Docker container'){

            steps {

                sh """
                docker run -itd --cpus="0.5"  --memory="0.5g" -v /data/workspace/singer-tap_dev:/app --name tap  "tap:v1"
                docker exec  "mvntest-${suffix}"  chmod -R ug=rwX,g+s /project
                """
                }
            post {
                success {
                          slackSend channel: '#ops',
                          color: '#0000FF',
                          message: " Docker container created  "
                       }

                failure {
                          slackSend channel: '#ops',
                          color: '#0000FF',
                          message: " Docker container failed to  created  "
                        }

                }
            }
        stage ('singet tap commands execution ') {
            steps {
                docker exec tap sh -c 'python3 -c "import os, json; print(json.dumps(dict(os.environ), indent=2))" > /app/ordway-tap/config.json'
                docker exec tap sh -c 'cat /app/ordway-tap/config.json'
                docker exec tap sh -c 'tap-ordway -c /app/ordway-tap/config.json --catalog /app/ordway-tap/catalog.json  | /usr/local/bin/target-stitch  --config /app/ordway-tap/stitch_config.json'
        
                }
            post {
                success {
                          slackSend channel: '#ops',
                          color: '#0000FF',
                          message: " MVN clean and package executed on container "
                       }

                failure {
                          slackSend channel: '#ops',
                          color: '#0000FF',
                          message: " MVN clean and package failed on container "
                        }

                 }
            }
        

        stage ('delete containers') {
            steps{
                   sh 'docker stop $(docker ps -a | grep "tap" | awk \'{print $1}\')      || true'
                   sh 'docker rm  $(docker ps -aq) || true'
                   sh 'docker image prune -a --force      || true'

                }
            post {
                success {
                          slackSend channel: '#ops',
                          color: '#0000FF',
                          message: "Deleted images and containers "

                        }

                failure {
                         slackSend channel: '#ops',
                         color: '#0000FF',
                         message: " Failed to deleted images and containers "
                        }
                }
            }
        }
post {
        failure {
                sh """
                docker stop \$(docker ps -a | grep "mvntest-${suffix}" | awk '{print \$1}') || true
                docker rm \$(docker ps -aq) || true
                """
        }
    }
}   
