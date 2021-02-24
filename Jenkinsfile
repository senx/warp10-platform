#!/usr/bin/env groovy
//
//   Copyright 2018-2021  SenX S.A.S.
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
        THRIFT_HOME = '/opt/thrift-0.11.0'
        GRADLE_CMD = "./gradlew -Psigning.gnupg.keyName=${getParam('gpgKeyName')} -PossrhUsername=${getParam('ossrhUsername')} -PossrhPassword=${getParam('ossrhPassword')}"
        VERSION = "UNKNOWN"
    }
    stages {

        stage('Checkout') {
            steps {
                notifyBuild('STARTED')
                git credentialsId: "${getParam('gitCredentials')}", poll: false, branch: "${getParam('gitBranch')}", url: "${getParam('gitUrl')}"
                sh 'git fetch --tags'
                script {
                    VERSION = getVersion()
                }
                echo "Building ${VERSION}"
            }
        }

        stage('Build') {
            steps {
                sh '$GRADLE_CMD clean build -x test'
                sh '$GRADLE_CMD generateChangelog'
                archiveArtifacts allowEmptyArchive: true, artifacts: '**/build/libs/*.jar', fingerprint: true
            }
        }

        stage('Test') {
            options { retry(3) }
            steps {
                sh '$GRADLE_CMD -Djava.security.egd=file:/dev/urandom test'
                junit allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/**/*.xml'
                step([$class: 'JUnitResultArchiver', allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/**/*.xml'])
            }
        }


        stage('Tar') {
            steps {
                sh '$GRADLE_CMD createTarArchive -x test'
                archiveArtifacts allowEmptyArchive: true, artifacts: '**/build/libs/*.tar.gz', fingerprint: true
            }
        }

        stage('Deploy libs to SenX\' Nexus') {
            steps {
                nexusPublisher nexusInstanceId: 'nex', nexusRepositoryId: 'maven-releases', packages: [
                  [
                    $class         : 'MavenPackage',
                    mavenAssetList : [
                      [classifier: ''       , extension: 'jar', filePath: 'crypto/build/libs/crypto-' + VERSION + '.jar'],
                      [classifier: 'sources', extension: 'jar', filePath: 'crypto/build/libs/crypto-' + VERSION + '-sources.jar'],
                      [classifier: 'javadoc', extension: 'jar', filePath: 'crypto/build/libs/crypto-' + VERSION + '-javadoc.jar']
                    ],
                    mavenCoordinate: [artifactId: 'crypto', groupId: 'io.warp10', packaging: 'jar', version: VERSION ]
                  ],
                  [
                    $class         : 'MavenPackage',
                    mavenAssetList : [
                      [classifier: ''       , extension: 'jar', filePath: 'token/build/libs/token-' + VERSION + '.jar'],
                      [classifier: 'sources', extension: 'jar', filePath: 'token/build/libs/token-' + VERSION + '-sources.jar'],
                      [classifier: 'javadoc', extension: 'jar', filePath: 'token/build/libs/token-' + VERSION + '-javadoc.jar']
                    ],
                    mavenCoordinate: [artifactId: 'token', groupId: 'io.warp10', packaging: 'jar', version: VERSION ]
                  ],
                  [
                    $class         : 'MavenPackage',
                    mavenAssetList : [
                      [classifier: ''       , extension: 'jar', filePath: 'hbaseFilters/build/libs/hbaseFilters-' + VERSION + '.jar'],
                      [classifier: 'sources', extension: 'jar', filePath: 'hbaseFilters/build/libs/hbaseFilters-' + VERSION + '-sources.jar'],
                      [classifier: 'javadoc', extension: 'jar', filePath: 'hbaseFilters/build/libs/hbaseFilters-' + VERSION + '-javadoc.jar']
                    ],
                    mavenCoordinate: [artifactId: 'hbaseFilters', groupId: 'io.warp10', packaging: 'jar', version: VERSION ]
                  ],
                  [
                    $class         : 'MavenPackage',
                    mavenAssetList : [
                      [classifier: ''       , extension: 'jar', filePath: 'warpscript/build/libs/warpscript-' + VERSION + '.jar'],
                      [classifier: 'sources', extension: 'jar', filePath: 'warpscript/build/libs/warpscript-' + VERSION + '-sources.jar'],
                      [classifier: 'javadoc', extension: 'jar', filePath: 'warpscript/build/libs/warpscript-' + VERSION + '-javadoc.jar']
                    ],
                    mavenCoordinate: [artifactId: 'warpscript', groupId: 'io.warp10', packaging: 'jar', version: VERSION ]
                  ],
                ]
            }
        }

        stage('Publish') {
            /* when {
                 expression { return isItATagCommit() }
             }*/
            parallel {
                stage('Deploy to Maven Central') {
                    options {
                        timeout(time: 2, unit: 'HOURS')
                    }
                    input {
                        message 'Should we deploy to Maven Central?'
                    }
                    steps {
                        sh '$GRADLE_CMD uploadArchives'
                        sh '$GRADLE_CMD closeAndReleaseRepository'
                        notifyBuild('PUBLISHED')
                    }
                }
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
    String subject = "${buildStatus}: Job ${env.JOB_NAME} [${env.BUILD_DISPLAY_NAME}] | ${env.VERSION}"
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

String getParam(key) {
    return params.get(key)
}

String getVersion() {
    return sh(returnStdout: true, script: '$GRADLE_CMD --quiet version').trim()
}

boolean isItATagCommit() {
    String lastCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
    String tag = sh(returnStdout: true, script: "git show-ref --tags -d | grep ^${lastCommit} | sed -e 's,.* refs/tags/,,' -e 's/\\^{}//'").trim()
    return tag != ''
}
