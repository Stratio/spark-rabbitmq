@Library('libpipelines@feature/multibranch') _

hose {
    EMAIL = 'sparta'
    MODULE = 'Spark-RabbitMQ'
    DEVTIMEOUT = 20
    RELEASETIMEOUT = 20
    FOSS = true
    REPOSITORY = 'Spark-RabbitMQ'

    ITSERVICES = [
        ['RABBITMQ': [
           'image': 'rabbitmq:3.6.1-management'           
        ]        
      ]
      
    ITPARAMETERS = "-Drabbitmq.hosts=%%RABBITMQ"
      
    DEV = { config ->
            doCompile(config)
            doIT(config)
            doPackage(config)
                        
            parallel(QC: {
                doStaticAnalysis(config)
            }, DEPLOY: {
                doDeploy(config)
            }, failFast: lib.vars.FAILFAST)        
    }
}
