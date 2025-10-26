pipeline {
    agent { label 'master' }

    environment {
        MODULE_NAME             = 'etl-ingest-common'
        GIT_BRANCH              = 'develop'
        GIT_URL                 = 'https://example.com/data/etl-ingest-common.git'
        REGISTRY_PATH           = 'etl/dev'
        SONAR_HOME              = tool 'SonarScanner'
        DATABRICKS_CONFIG_FILE  = '/data/jenkins/tools/databricks/databrickscfg.dev'
        LINE_TOKEN              = credentials('line-token-devops')
        GIT_CREDENTIALS         = 'devsecopsuser'
        ARTIFACT_CREDENTIALS    = 'artifactory-service'
    }

    stages {
        stage('Checkout Sources') {
            steps {
                deleteDir()
                withCredentials([usernameColonPassword(credentialsId: env.GIT_CREDENTIALS, variable: 'GIT_AUTH')]) {
                    git branch: env.GIT_BRANCH, changelog: false, credentialsId: env.GIT_CREDENTIALS, poll: false, url: env.GIT_URL
                }
                sh '''
                    COMMIT_ID=$(git rev-parse HEAD | cut -c1-8)
                    echo "$COMMIT_ID" > commit.txt
                '''
                script {
                    def commit = readFile('commit.txt').trim()
                    currentBuild.displayName = "#${env.BUILD_NUMBER}_${commit}"
                }
            }
        }

        stage('Static Analysis (SonarQube)') {
            steps {
                withSonarQubeEnv('sonarqube-main') {
                    sh '''
                        COMMIT_ID=$(cat commit.txt)
                        ${SONAR_HOME}/bin/sonar-scanner \
                          -Dsonar.projectKey=ETL.${MODULE_NAME} \
                          -Dsonar.projectName=ETL.${MODULE_NAME} \
                          -Dsonar.projectVersion=${COMMIT_ID} \
                          -Dsonar.sources=. \
                          -Dsonar.ws.timeout=300
                    '''
                }
            }
        }

        stage('Quality Gate') {
            steps {
                timeout(time: 5, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }

        stage('Databricks Bundle Validate') {
            steps {
                withCredentials([usernameColonPassword(credentialsId: env.GIT_CREDENTIALS, variable: 'GIT_AUTH')]) {
                    dir('config') {
                        git branch: env.GIT_BRANCH, credentialsId: env.GIT_CREDENTIALS, poll: false, url: 'https://example.com/data/etl-ingest-common-config.git'
                    }
                }
                sh '''
                    cp -r config/databricks_config/* .
                    databricks bundle validate --target dev
                '''
            }
        }

        stage('Deploy to Databricks') {
            steps {
                withEnv(["DATABRICKS_CONFIG_FILE=${env.DATABRICKS_CONFIG_FILE}"]) {
                    timeout(time: 30, unit: 'MINUTES') {
                        sh 'databricks bundle deploy --target dev'
                    }
                }
            }
        }

        stage('Archive Artifact') {
            steps {
                withCredentials([usernameColonPassword(credentialsId: env.ARTIFACT_CREDENTIALS, variable: 'ARTIFACT_AUTH')]) {
                    sh '''
                        COMMIT_ID=$(cat commit.txt)
                        mkdir ${MODULE_NAME}
                        rsync -a --exclude '.databricks' ./ ${MODULE_NAME}/
                        tar -czf ${MODULE_NAME}_${COMMIT_ID}.tar.gz ${MODULE_NAME}
                        MD5=$(md5sum ${MODULE_NAME}_${COMMIT_ID}.tar.gz | awk '{print $1}')
                        SHA1=$(sha1sum ${MODULE_NAME}_${COMMIT_ID}.tar.gz | awk '{print $1}')
                        curl -u${ARTIFACT_AUTH} --fail --insecure \
                            --header "X-Checksum-MD5:${MD5}" \
                            --header "X-Checksum-Sha1:${SHA1}" \
                            -T ${MODULE_NAME}_${COMMIT_ID}.tar.gz \
                            "https://artifactory.example.com/${REGISTRY_PATH}/databricks/${MODULE_NAME}/${MODULE_NAME}_${COMMIT_ID}.tar.gz"
                    '''
                }
            }
        }
    }

    post {
        success {
            emailext subject: "${env.JOB_NAME}: Completed", body: 'Sent by Jenkins', to: env.MAIL_TEST ?: ''
        }
        failure {
            emailext subject: "${env.JOB_NAME}: Failed", body: 'Sent by Jenkins', to: env.MAIL_TEST ?: ''
        }
    }
}
