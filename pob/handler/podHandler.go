package handler

import (
	"context"
	"pob/common"
	"pob/domain/model"
	"pob/domain/service"
	"pob/proto/pod"
	"strconv"
)

type PodHandler struct {
	//接口
	PodDataService service.IPodDataService
}

// 添加pod
func (e *PodHandler) AddPod(ctx context.Context, info *pod.PodInfo, rsp *pod.Response) error {
	common.Info("添加pod")
	podModel := &model.Pod{}
	err := common.SwapTo(info, podModel)
	if err != nil {
		common.Error(err)
		rsp.Msg = err.Error()
		return err
	}

	if err = e.PodDataService.CreateToK8s(info); err != nil {
		common.Error(err)
		rsp.Msg = err.Error()
		return err
	} else {
		podID, err := e.PodDataService.AddPod(podModel)
		if err != nil {
			common.Error(err)
			rsp.Msg = err.Error()
			return err
		}
		common.Info("Pod添加成功 ID:" + strconv.FormatInt(podID, 10))
		rsp.Msg = "Pod添加成功 ID:" + strconv.FormatInt(podID, 10)
	}
	return nil
}

func (e *PodHandler) DeletePod(ctx context.Context, req *pod.PodId, rsp *pod.Response) error {
	podModel, err := e.PodDataService.FindPodByID(req.Id)
	if err != nil {
		common.Error(err)
		rsp.Msg = err.Error()
		return err
	}
	err = e.PodDataService.DeleteFromK8s(podModel)
	if err != nil {
		common.Error(err)
		rsp.Msg = err.Error()
		return err
	}
	return nil
}

func (e *PodHandler) UpdatePod(ctx context.Context, req *pod.PodInfo, rsp *pod.Response) error {
	err := e.PodDataService.UpdateToK8s(req)
	if err != nil {
		common.Error(err)
		rsp.Msg = err.Error()
		return err
	}
	podModel, err := e.PodDataService.FindPodByID(req.Id)
	if err != nil {
		common.Error(err)
		rsp.Msg = err.Error()
		return err
	}
	err = common.SwapTo(req, podModel)
	if err != nil {
		common.Error(err)
		rsp.Msg = err.Error()
		return err
	}
	err = e.PodDataService.UpdatePod(podModel)
	if err != nil {
		common.Error(err)
		rsp.Msg = err.Error()
		return err
	}
	return nil
}

func (e *PodHandler) FindPodByID(ctx context.Context, req *pod.PodId, rsp *pod.PodInfo) error {
	podModel, err := e.PodDataService.FindPodByID(req.Id)
	if err != nil {
		common.Error(err)
		return err
	}
	err = common.SwapTo(req, podModel)
	if err != nil {
		common.Error(err)
		return err
	}
	return nil
}

func (e *PodHandler) FindAllPod(ctx context.Context, req *pod.FindAll, rsp *pod.AllPod) error {
	allPod, err := e.PodDataService.FindAll()
	if err != nil {
		common.Error(err)
		return err
	}
	//整理
	for _, v := range allPod {
		podInfo := &pod.PodInfo{}
		err = common.SwapTo(v, podInfo)
		if err != nil {
			common.Error(err)
			return err
		}
		rsp.PodInfo = append(rsp.PodInfo, podInfo)
	}
	return nil
}
