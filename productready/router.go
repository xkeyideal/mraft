package productready

import "github.com/gin-gonic/gin"

func (engine *Engine) registerRouter(router *gin.Engine) {
	group := router.Group(engine.prefix)
	{
		group.GET("/info", engine.kvHandle.Info)

		group.GET("/key", engine.kvHandle.Query)
		group.POST("/key", engine.kvHandle.Upsert)
		group.DELETE("/key", engine.kvHandle.Delete)

		group.GET("/join", engine.kvHandle.JoinNode)
		group.GET("/del", engine.kvHandle.DelNode)
	}
}
