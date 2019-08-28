package engine

import "github.com/gin-gonic/gin"

func (engine *Engine) registerMraftRouter(router *gin.Engine) {
	group := router.Group(engine.prefix)
	{
		group.GET("/info", engine.mraftHandle.Info)
		group.GET("/metrics", engine.mraftHandle.RaftMetrics)

		group.GET("/key", engine.mraftHandle.Query)
		group.POST("/key", engine.mraftHandle.Upsert)
		group.DELETE("/key", engine.mraftHandle.Delete)
	}
}
