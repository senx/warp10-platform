#!/usr/bin/env groovy
import hudson.model.*

pipeline {
    agent any
    environment {
        THRIFT_HOME = '/opt/thrift-0.9.1'
        version = this.getVersion()
        BINTRAY_USER = getParam('BINTRAY_USER')
        BINTRAY_API_KEY = getParam('BINTRAY_API_KEY')
    }
    stages {

        stage('Checkout') {
            steps {
                this.notifyBuild('STARTED', version)
                git credentialsId: 'github', url: 'git@github.com:Giwi/warp10-platform.git'
                echo "Building ${version}"
            }
        }

        stage('Build') {
            steps {
                sh './gradlew clean crypto:install token:install build -x test'
            }
        }

        stage('Test') {
            steps {
                sh './gradlew test'
                junit allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/**/*.xml'
            }
        }


        stage('Pack') {
            steps {
                sh './gradlew jar pack -x test'
                archiveArtifacts allowEmptyArchive: true, artifacts: '**/build/libs/*.jar', fingerprint: true
            }
        }

        stage('Deploy') {
            input {
                message "Should we deploy to Bintray?"
            }
            steps {
                sh './gradlew crypto:clean crypto:bintrayUpload -x test'
                sh './gradlew token:clean  token:bintrayUpload -x test'
                sh './gradlew warp10:clean warp10:bintrayUpload -x test'
            }
        }
    }

    post {
        success {
            this.notifyBuild('SUCCESSFUL', version)
        }
        failure {
            this.notifyBuild('FAILURE', version)
        }
        aborted {
            this.notifyBuild('ABORTED', version)
        }
        unstable {
            this.notifyBuild('UNSTABLE', version)
        }
    }
}

void notifyBuild(String buildStatus, String version) {
    // build status of null means successful
    buildStatus = buildStatus ?: 'SUCCESSFUL'
    String subject = "${buildStatus}: Job ${env.JOB_NAME} [${env.BUILD_NUMBER}] | ${version}"
    String summary = "${subject} (${env.BUILD_URL})"
    // Override default values based on build status
    if (buildStatus == 'STARTED') {
        color = 'YELLOW'
        colorCode = '#FFFF00'
    } else if (buildStatus == 'SUCCESSFUL') {
        color = 'GREEN'
        colorCode = '#00FF00'
    } else if (buildStatus == 'PUBLISHED') {
        color = 'BLUE'
        colorCode = '#0000FF'
    } else {
        color = 'RED'
        colorCode = '#FF0000'
    }

    // Send notifications
    this.notifySlack(colorCode, summary, buildStatus)
}

def notifySlack(color, message, buildStatus) {
    String slackURL = 'https://hooks.slack.com/services/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'
    String payload = "{\"username\": \"Warp10\",\"attachments\":[{\"title\": \"${env.JOB_NAME} ${buildStatus}\",\"color\": \"${color}\",\"text\": \"${message}\"}]}"
    def cmd = "curl -X POST -H 'Content-type: application/json' --data '${payload}' ${slackURL}"
    print cmd
    sh cmd
}

String getParam(key) {
    return param[key]
}

String getVersion() {
    return sh(returnStdout: true, script: 'git describe --abbrev=0 --tags').trim()
}