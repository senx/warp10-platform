#!/usr/bin/env groovy
import hudson.model.*

pipeline {
    agent any
    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '3'))
    }

    environment {
        GRADLE_ARGS = "-Psigning.gnupg.keyName=${getParam('gpgKeyName')} -PossrhUsername=${getParam('ossrhUsername')} -PossrhPassword=${getParam('ossrhPassword')} -PnexusHost=${getParam('nexusHost')}  -PnexusUsername=${getParam('nexusUsername')} -PnexusPassword=${getParam('nexusPassword')}"
        version = "${getVersion()}"
    }
    stages {
        stage('Checkout') {
            steps {
                this.notifyBuild('STARTED', version)
                git poll: false, url: 'git@github.com:senx/warp10-ext-interpolation.git'
                sh 'git checkout master'
                sh 'git fetch --tags'
                sh 'git pull origin master'
            }
        }

        stage('Build') {
            steps {
                sh './gradlew clean build $GRADLE_ARGS'
            }
        }

        stage('Package') {
            steps {
                sh './gradlew -Duberjar shadowJar sourcesJar javadocJar $GRADLE_ARGS'
                archiveArtifacts "build/libs/*.jar"
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
                sh './gradlew $GRADLE_ARGS -Duberjar shadowJar sourcesJar javadocJar publishUberJarPublicationToNexusRepository -x test'
                sh './gradlew $GRADLE_ARGS -Duberjar clean jar sourcesJar javadocJar publishMavenPublicationToNexusRepository -x test -x shadowJar'
            }
        }

        stage('Maven Publish') {
            when {
                expression { return isItATagCommit() }
            }
            parallel {
                stage('Deploy to Maven Central') {
                    options {
                        timeout(time: 2, unit: 'HOURS')
                    }
                    input {
                        message 'Should we deploy to Maven Central?'
                    }
                    steps {
                        sh './gradlew -Duberjar clean shadowJar sourcesJar javadocJar publishUberJarPublicationToMavenRepository -x test $GRADLE_ARGS'
                        sh './gradlew clean jar sourcesJar javadocJar publishMavenPublicationToMavenRepository -x test -x shadowJar $GRADLE_ARGS'
                        sh './gradlew closeRepository $GRADLE_ARGS'
                        sh './gradlew releaseRepository $GRADLE_ARGS'
                        this.notifyBuild('PUBLISHED', version)
                    }
                }
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
    String subject = "${buildStatus}: Job ${env.JOB_NAME} [${env.BUILD_DISPLAY_NAME}] | ${version}" as String
    String summary = "${subject} (${env.BUILD_URL})" as String
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

void notifySlack(String color, String message, String buildStatus) {
    String slackURL = getParam('slackUrl')
    String payload = "{\"username\": \"${env.JOB_NAME}\",\"attachments\":[{\"title\": \"${env.JOB_NAME} ${buildStatus}\",\"color\": \"${color}\",\"text\": \"${message}\"}]}" as String
    sh "curl -X POST -H 'Content-type: application/json' --data '${payload}' ${slackURL}" as String
}

String getParam(String key) {
    return params.get(key)
}

String getVersion() {
    return sh(returnStdout: true, script: 'git describe --abbrev=0 --tags').trim()
}

boolean isItATagCommit() {
    String lastCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
    String tag = sh(returnStdout: true, script: "git show-ref --tags -d | grep ^${lastCommit} | sed -e 's,.* refs/tags/,,' -e 's/\\^{}//'").trim()
    return tag != ''
}