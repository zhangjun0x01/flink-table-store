


# paimon加密简介


paimon提供了一个插件化的加密机制来对数据文件（orc and parquet format are supported, avro is not supported）进行加密，
用户可以在创建表的时候通过指定相应的参数来实现不同的加密机制。
并可以通过实现相应的接口来方便的扩展加密机制和kms client。

系统提供了信封加密的机制来实现对数据的加解密功能，尽可能少的减少和kms的交互，既满足功能，也提高吞吐量。

当进行写操作的时候，会首先从kms获取一个master key，这个key是表级别的，一个表一个master key，其对应的key id存储在snapshot里面。
使用master key生成 local data key，用于加密数据文件，加密后的data key保存在对应的manifest里面。

当进行读操作的时候，首先通过master key id从kms获取对应的明文master key，用其解密对应的data key，然后再读取相应的数据文件。

# 加密机制

提供对数据文件的加密机制，目前支持纯文本模式（默认）和信封加密模式。

## plaintext

提供纯文本的加密模式，也就是不对数据进行加密，该模式是默认模式。

## 信封加密

提供信封加密的模式来对数据进行加密。用户可以通过设置参数 `'encryption.mechanism' = 'envelope'` 来开启信封加密功能

# KMS Client

master key存储到kms里面，该接口提供了对kms服务的访问以及一些常见的操作，比如加密、解密。

## hadoop kms

提供了对hadoop kms 的访问，`hadoop.security.key.provider.path` 是必填项，用于指定hadoop kms的访问地址，用户还可以添加其他的以hadoop.security.开头的hadoop参数，具体的可以参考hadoop的官网
https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/core-default.xml

## memory

提供了一个基于内存的KMS服务，该kms仅仅用于单元测试类进行测试使用，不能用于生产环境。

# 部分列机密

paimon提供只对部分敏感列的加密，用户可以通过 `encryption.columns` 来指定要加密的列，多个列用逗号分隔。

# 加密算法

用户可以通过 `encryption.algorithm` 参数来指定对数据文件的加密算法，如果是parquet格式，可以选择`AES_GCM_V1` or `AES_GCM_CTR_V1`，默认是 `AES_GCM_CTR_V1`.
对于orc格式，目前无法选择，只支持AES_CTR算法。




