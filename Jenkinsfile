#!/usr/bin/env groovy
//
//   Copyright 2018-2023  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

import hudson.model.*

pipeline {
    agent any
    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '3'))
    }
    environment {
        THRIFT_HOME = '/opt/thrift-0.17.0'
        GPG_KEY_NAME = "${params.gpgKeyName}"
        NEXUS_HOST = "${params.nexusHost}"
        NEXUS_CREDS = credentials('nexus')
        OSSRH_CREDS = credentials('ossrh')
        GRADLE_CMD = './gradlew --no-parallel -Psigning.gnupg.keyName=$GPG_KEY_NAME -PossrhUsername=$OSSRH_CREDS_USR -PossrhPassword=$OSSRH_CREDS_PSW -PnexusHost=$NEXUS_HOST  -PnexusUsername=$NEXUS_CREDS_USR -PnexusPassword=$NEXUS_CREDS_PSW'
    }
    stages {

        stage('Checkout') {
            steps {
                notifyBuild('STARTED')
                checkout scm
                script {
                    VERSION = getVersion()
                    TAG = getTag()
                }
                echo "Building ${VERSION}"
            }
        }

        stage('Build') {
            steps {
                sh "$GRADLE_CMD build -x test"
                archiveArtifacts allowEmptyArchive: true, artifacts: '**/build/libs/*.jar', fingerprint: true
            }
        }

        stage('Test') {
            options { retry(3) }
            steps {
                sh "$GRADLE_CMD -Djava.security.egd=file:/dev/urandom test"
                junit allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/**/*.xml'
                step([$class: 'JUnitResultArchiver', allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/**/*.xml'])
            }
        }

        stage('Tar') {
            steps {
                sh "$GRADLE_CMD createTarArchive -x test"
                archiveArtifacts allowEmptyArchive: true, artifacts: '**/build/libs/*.tar.gz', fingerprint: true
            }
        }

        stage('Deploy libs to SenX\' Nexus') {
            options {
                timeout(time: 2, unit: 'HOURS')
            }
            input {
                message "Should we deploy libs?"
            }
            steps {
                sh "$GRADLE_CMD publishMavenPublicationToNexusRepository -x test"
            }
        }

        stage('Release tar.gz on GitHub') {
            when {
                beforeInput true
                // Only possible if code pulled from github because the release will refer to the
                // given tag in the given branch. If no such tag exists, it is created from the
                // HEAD of the branch.
                expression { return 'github.com' == getParam('gitHost') }
            }
            options {
                timeout(time: 2, unit: 'HOURS')
            }
            input {
                message "Should we release Warp 10?"
            }
            steps {
                script {
                    releaseID = createGitHubRelease()
                }
                sh "curl -f -X POST -H \"Authorization:token ${getParam('githubToken')}\" -H \"Content-Type:application/octet-stream\" -T warp10/build/libs/warp10-${VERSION}.tar.gz https://uploads.github.com/repos/${getParam('gitOwner')}/${getParam('gitRepo')}/releases/${releaseID}/assets?name=warp10-${VERSION}.tar.gz"
            }
        }

        stage('Deploy libs to Maven Central') {
            options {
                timeout(time: 2, unit: 'HOURS')
            }
            input {
                message "Should we deploy libs?"
            }
            steps {
                sh "$GRADLE_CMD publish"
                sh "$GRADLE_CMD closeRepository"
                sh "$GRADLE_CMD releaseRepository"
                notifyBuild('PUBLISHED')
            }
        }

    }

    post {
        success {
            notifyBuild('SUCCESSFUL')
        }
        failure {
            notifyBuild('FAILURE')
        }
        aborted {
            notifyBuild('ABORTED')
        }
        unstable {
            notifyBuild('UNSTABLE')
        }
    }
}

void notifyBuild(String buildStatus) {
    // build status of null means successful
    buildStatus = buildStatus ?: 'SUCCESSFUL'
    String subject = "${buildStatus}: Job ${env.JOB_NAME} [${env.BUILD_DISPLAY_NAME}]"
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
    notifySlack(colorCode, summary, buildStatus)
}

void notifySlack(color, message, buildStatus) {
    String slackURL = getParam('slackUrl')
    String payload = "{\"username\": \"${env.JOB_NAME}\",\"attachments\":[{\"title\": \"${env.JOB_NAME} ${buildStatus}\",\"color\": \"${color}\",\"text\": \"${message}\"}]}"
    sh "curl -X POST -H 'Content-type: application/json' --data '${payload}' ${slackURL}"
}

String createGitHubRelease() {
    String githubURL = "https://api.github.com/repos/${getParam('gitOwner')}/${getParam('gitRepo')}/releases"
    String payload = "{\"tag_name\": \"${VERSION}\", \"name\": \"${VERSION}\", \"body\": \"Release ${VERSION}\", \"target_commitish\": \"${getParam('gitBranch')}\", \"draft\": false, \"prerelease\": false}"
    releaseID = sh (returnStdout: true, script: "curl -f -X POST -H \"Authorization:token ${getParam('githubToken')} \" --data '${payload}' ${githubURL} | sed -n -e 's/\"id\":\\ \\([0-9]\\+\\),/\\1/p' | head -n 1").trim()
    return releaseID
}

String getParam(key) {
    return params.get(key)
}

String getVersion() {
    return sh(returnStdout: true, script: '$GRADLE_CMD --quiet version').trim()
}

String getTag() {
    return sh(returnStdout: true, script: 'git describe --tags').trim()
}
