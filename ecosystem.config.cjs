module.exports = {
    apps: [{
        name: 'kh-pbtc-server',
        script: 'src/server.js',
        //cwd: 'file:///E/project/kh-pbtc-server',
        args: '',
        instances: 1,
        exec_mode: 'fork',
        autorestart: true,
        watch: false,
        //max_memory_restart: '1G',
        env_production: {
            NODE_ENV: 'production',
            DEBUG: null,
        },
    }],
};
