@Library("telegram-notification@master")

import com.romy.telegram.Telegram

pipeline {
    agent {
        node { 
            label 'docker && kubectl' 
        }
    }
    environment {
        CONFIG = 'config.yaml'
    }
    parameters {
      text(name: 'IMAGE_TAG', defaultValue: '0.0.1', description: 'image tag') 
    }
    stages {
        stage('Prepare Pipeline') {
            steps {
                script {
                    telegramClient = new Telegram(
                        TELEGRAM_BOT_TOKEN,
                        TELEGRAM_CHAT_ID,
                        TELEGRAM_MESSAGE_THREAD_ID,
                        this
                    )
                    FAILED_STAGE = STAGE_NAME
                    telegramClient.sendMessage("ℹ️ *pipeline start* \n\n*job*\t: ${JOB_NAME}\n*node*\t: ${NODE_NAME}\n*build*\t: #${BUILD_NUMBER}")
                    
                    def data = readYaml file: CONFIG
                    project = data.project 
                    
                    telegramClient.sendMessage(
                        "ℹ️ *load config* \n\n*name*\t: ${project.name}\n*pic*\t: ${project.pic}\n*kube*\t: ${project.kubernetes.cluster} ( ${project.kubernetes.namespace} )"
                    )
                }
            }
        }
        stage('Build Image') {
            steps {
                script {
                    FAILED_STAGE = STAGE_NAME
                    imageName = docker.build(
                        "${project.docker.registry.address}/${project.docker.namespace}/${project.docker.image.name}:${IMAGE_TAG}"
                    ).imageName()
                    telegramClient.sendMessage("✅ *build success* ${imageName}")
                }
            }
        }
        
        stage('Push Image') {
            steps {
                script {
                    FAILED_STAGE = STAGE_NAME
                    docker.image(
                        imageName
                    ).push()
                    telegramClient.sendMessage("✅ *push harbor success* ${imageName}")
                }
            }
        }

        stage('Deploy Kubernetes') {
            steps {
                script {
                    FAILED_STAGE = STAGE_NAME
                    def kubeconfig = env.getProperty("KUBE_CONFIG_" + project.kubernetes.cluster.toUpperCase())
                    def messages = sh(
                        script: """
                        sed -i 's|image: .*|image: ${imageName}|' ${project.deployment_base_dir}/*.yaml
                        kubectl apply -f ${project.deployment_base_dir} -n ${project.kubernetes.namespace} --kubeconfig ${kubeconfig}
                        """,
                        returnStdout: true
                    ).trim()
                    telegramClient.sendMessage("✅ *successfully applied*")
                    for (message in messages.split('\n')) {
                        telegramClient.sendMessage("✅ ${message}")
                    }
                }
            }
        }
    }
    post {
        success {
            script {
                telegramClient.sendMessage("✅ *pipeline success* ${JOB_NAME} #${BUILD_NUMBER}")
            }
        }
        failure {
            script {
                telegramClient.sendMessage("❌ *pipeline failed* ${JOB_NAME}\n\n*stage*\t: ${FAILED_STAGE}\n*build*\t: #${BUILD_NUMBER}")
            }
        }
    }
}
