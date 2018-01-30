#!/usr/bin/env groovy
import hudson.model.*

pipeline {
    environment {
        THRIFT_HOME = '/opt/thrift-0.9.1'
    }
    stages {
        def version = this.version()

        //   this.notifyBuild('STARTED')

        stage('Checkout') {
            steps {
                git credentialsId: 'github', url: 'git@github.com:Giwi/warp10-platform.git'
                echo("Building $version")
            }
        }

        stage('Build ' + version) {
            steps {
                sh './gradlew clean crypto:install token:install build -x test'
            }
        }

        stage('Test ' + version) {
            steps {
                sh './gradlew test'
                junit allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/reports/**/*.xml'
            }
        }


        stage('Pack ' + version) {
            steps {
                sh './gradlew warp10:pack '
            }
        }

        post {
            success {
                //      this.notifyBuild(currentBuild.result)
            }
            failure {
                currentBuild.result = "FAILED"
                //      this.notifyBuild(currentBuild.result)
            }
        }
    }
}

def notifyBuild(String buildStatus = 'STARTED') {
    // build status of null means successful
    buildStatus = buildStatus ?: 'SUCCESSFUL'
    String subject = "${buildStatus}: Job ${env.JOB_NAME} [${env.BUILD_NUMBER}]"
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


def version() {
    return sh(returnStdout: true, script: 'git describe --abbrev=0 --tags').trim()
}
