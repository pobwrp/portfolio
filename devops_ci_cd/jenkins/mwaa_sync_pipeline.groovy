pipeline {
    agent { label 'master' }

    environment {
        MODULE_NAME        = 'airflow-mwaa'
        GIT_BRANCH         = 'main'
        GIT_URL            = 'https://example.com/data/airflow.git'
        S3_BUCKET          = 's3://portfolio-mwaa-artifacts'
        MWAA_ENV           = 'portfolio-airflow'
        AWS_DEFAULT_REGION = 'ap-southeast-1'
        GIT_CREDENTIALS    = 'devsecopsuser'
        AWS_CREDENTIALS    = 'aws-shared-creds'
    }

    stages {
        stage('Checkout') {
            steps {
                deleteDir()
                withCredentials([usernameColonPassword(credentialsId: env.GIT_CREDENTIALS, variable: 'GIT_AUTH')]) {
                    git branch: env.GIT_BRANCH, credentialsId: env.GIT_CREDENTIALS, poll: false, url: env.GIT_URL
                }
                sh 'echo $(git rev-parse HEAD | cut -c1-8) > commit.txt'
            }
        }

        stage('Package Artifacts') {
            steps {
                sh '''
                    mkdir -p build
                    rsync -a dags/ build/dags/
                    rsync -a plugins/ build/plugins/ || true
                    rsync -a requirements/ build/requirements/ || true
                '''
            }
        }

        stage('Sync to S3') {
            steps {
                withAWS(credentials: env.AWS_CREDENTIALS, region: env.AWS_DEFAULT_REGION) {
                    sh '''
                        aws s3 sync build ${S3_BUCKET}/${MODULE_NAME}/ \
                            --exclude '*' \
                            --include 'dags/*' \
                            --include 'plugins/*' \
                            --include 'requirements/*'
                    '''
                }
            }
        }

        stage('Trigger MWAA Sync') {
            steps {
                withAWS(credentials: env.AWS_CREDENTIALS, region: env.AWS_DEFAULT_REGION) {
                    sh '''
                        aws mwaa publish-metrics \
                            --environment-name ${MWAA_ENV} \
                            --metric-data name=dag_deploy,value=1
                    '''
                }
            }
        }
    }

    post {
        always {
            cleanWs()
        }
    }
}
