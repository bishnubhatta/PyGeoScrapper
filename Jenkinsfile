pipeline {
  agent any
  stages {
    stage('Code Review') {
      steps {
        sh '. ./python_code_review.ksh'
      }
    }
    stage('Archive Logs') {
      steps {
        parallel(
          "Archive Logs": {
            sh '. ./archive_log_files.ksh'
            
          },
          "Code review completion notification": {
            mail(subject: 'Code Review Complete Notification', body: 'Code review has completed for current build', to: 'bishnu.bhatta@gmail.com', from: 'JenkinsAdmin')
            
          }
        )
      }
    }
    stage('Email Rating Doc') {
      steps {
        emailext(attachmentsPattern: 'code_rating.txt', subject: 'Code Rating Txt', body: 'Code rating txt for current run attached', attachLog: true, to: 'bishnu.bhatta@gmail.com')
      }
    }
  }
}