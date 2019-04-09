/*
 * Copyright (c) Oscaro 2019 All rights reserved
 * This file and the information it contains are property of Oscaro and confidential.
 * They shall not be reproduced nor disclosed to any person except to those having
 * a need to know them without prior written consent of Oscaro.
 */

node {
    try {
            gitlabCommitStatus {
                def gitEnv = checkout(scm)
                def version = leinVersion()
                def buildNum = env.BUILD_NUMBER
                def branchName = gitEnv.GIT_BRANCH ?: branchName()
                def deployEnv = (branchName ==~ /.*master/) ? 'prod' : 'dev'
		
                echo """\
Git vars are $gitEnv
Version is '$version'
Build number is '$buildNum'
Branch name is '$branchName'
Deploy env is '$deployEnv'"""
                sh 'printenv'

                stage('Test') {
                    lein 'clean'
                    def port = 9092
                    docker
                      .image("spotify.kafka")
                      .withRun("-p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=172.17.0.1 --env ADVERTISED_PORT=9092")
                    { ctnr ->
                        lein 'test'
                    }
                }
                stage('Build')   { lein 'jar' }
                stage('Publish') { lein 'deploy'}
            }
    } finally {
        cleanWs()
    }
}