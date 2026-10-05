# koishi-plugin-opencode-modified

> [!WARNING]
> 本插件为自用插件，不在npm发布

本插件基于 [koishi-plugin-opencode](https://github.com/DoiiarX/koishi-plugin-opencode) 修改而来，因为本插件无法在最新版opencode正确使用，故对API进行迁移

## 安装

本修改版插件不在npm发布，感觉没啥必要，还麻烦，所以手动安装

### 操作步骤

> [!TIP]
> 如果对monorepo有所了解，就能很好理解为什么这么做

将本插件克隆到koishi开发项目的`external`或者`plugins`等任意一个workspace配置的文件夹中

安装依赖，然后运行`yakumo build`的命令构建插件

最终使用:

```bash
# npm i/yarn add/bun i...
pnpm i koishi-plugin-opencode-modified --workspace
```

即可正确安装在环境中，即可以在WebUI中编辑参数和查看日志等。
