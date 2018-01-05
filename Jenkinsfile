if (["dev", "master"].contains(env.BRANCH_NAME)) {
  node {
    checkout scm
    try {
      stage("Build") {
        sh "rm -rf dist"
        sh "python setup.py sdist bdist_wheel"
      }
      stage("Deploy") {
          def deployEnv = (env.BRANCH_NAME == "master") ? "prod" : "dev"
          def repositoryURL = "https://nexus.blackfynn.io/repository/pypi-$deployEnv/"
          withCredentials([
            usernamePassword(
              credentialsId: "blackfynn-nexus-ci-login",
              usernameVariable: "TWINE_USERNAME",
              passwordVariable: "TWINE_PASSWORD"
            )
          ]) {
            sh """
              twine upload dist/* \
              --repository-url $repositoryURL
            """
          }
          slackSend(color: '#006600', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL}) by ${env.CHANGE_AUTHOR}")
      }
    } catch (e) {
      slackSend(color: '#b20000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
      throw e
    }
  }
}
