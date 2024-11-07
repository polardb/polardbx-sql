# 打镜像

找一个机器，拉取代码，切换到自己的分支，然后打镜像

```
git fetch --depth=1 origin xxx

git checkout origin/xxx

cd polardbx-test/src/test/java/com/alibaba/polardbx/transfer/

docker build --network host -t transfer-test:v1.0.0 . -f docker/Dockerfile
```

# 运行

1. 准备一份 config.toml，挂载到容器内的 /tmp 目录，然后运行：

```
docker run --rm --network host \
-v /etc/localtime:/etc/localtime \
-v your-path:/tmp \
transfer-test:v1.0.0 \
-op run -config /tmp/config.toml
```

prepare 为导入数据，将 prepare 改成 run 就会运行测试。注意 config 文件里的 runmode 要改成 'docker'。
