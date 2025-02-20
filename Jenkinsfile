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
                        git reset --hard HEAD
                        git pull origin ${params.branch}
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
                      sh 'docker stop $(docker ps -a | grep "tap" | awk \'{print $1}\')      || true'
                      sh 'docker rm  $(docker ps -aq) || true'
                      sh 'docker image prune -a --force      || true'
                      sh "docker build -f /data/workspace/singer-tap_dev/Dockerfile -t tap:$suffix:latest ."
                  }
               }
             
    
}

        stage ('creating Docker container'){

            steps {

                sh """
                docker run -itd --cpus="0.5"  --memory="0.5g" -v /data/workspace/singer-tap_dev:/app --name tap "tap-$suffix:latest"
                """
                }
            
            }
        stage ('singet tap commands execution ') {
            steps {
                sh """
                docker exec tap sh -c 'python3 -c "import os, json; print(json.dumps(dict(os.environ), indent=2))" > /app/config.json'
                docker exec tap cat /app/config.json
                docker exec tap sh -c '
                tap-ordway -c /app/config.json --catalog /app/catalog.json | target-stitch --config /app/stitch_config.json'
                """
                }
            
            }
        

        stage ('delete containers') {
            steps{
                   sh 'docker stop $(docker ps -a | grep "tap" | awk \'{print $1}\')      || true'
                   sh 'docker rm  $(docker ps -aq) || true'
                   sh 'docker image prune -a --force      || true'
                }
            
            }
        }
post {
        failure {
                sh """
                docker stop \$(docker ps -a | grep tap | awk '{print \$1}') || true
                docker rm \$(docker ps -aq) || true
                """
        }
    }
}  
    
