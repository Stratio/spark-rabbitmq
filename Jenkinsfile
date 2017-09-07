@Library('libpipelines@master') _

hose {
    EMAIL = 'sparta'
    MODULE = 'spark-rabbitmq'
    DEVTIMEOUT = 70
    RELEASETIMEOUT = 40
    FOSS = true
    REPOSITORY = 'spark-rabbitmq'

    ITSERVICES = [
        ['RABBITMQ': [
           'image': 'rabbitmq:3-management'
        ]],      
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
            }, failFast: config.FAILFAST)        
    }
}
