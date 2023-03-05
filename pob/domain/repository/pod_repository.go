package repository

import (
	"github.com/jinzhu/gorm"
	"pob/domain/model"
)

type IPodRepository interface {
	//初始化表
	InitTable() error
	// 根据ID找数据
	FindPodByID(int64) (*model.Pod, error)
	// 创建Pod数据
	CreatePod(pod *model.Pod) (int64, error)
	//删除
	DeletePodByID(int642 int64) error
	//修改
	UpdatePod(pod *model.Pod) error
	// 查找所有Pod数据
	FindAll() ([]model.Pod, error)
}

func NewPodRepository(db *gorm.DB) IPodRepository {
	return &PodRepository{mysqlDb: db}
}

type PodRepository struct {
	mysqlDb *gorm.DB
}

func (u *PodRepository) InitTable() error {
	return u.mysqlDb.CreateTable(&model.Pod{}, &model.PodEnv{}, &model.PodPort{}).Error
}

func (u *PodRepository) FindPodByID(podID int64) (*model.Pod, error) {
	pod := &model.Pod{}
	return pod, u.mysqlDb.Preload("PodEnv").Preload("PodPort").First(pod, podID).Error
}

func (u *PodRepository) CreatePod(pod *model.Pod) (int64, error) {
	return pod.ID, u.mysqlDb.Create(pod).Error
}

func (u *PodRepository) DeletePodByID(podID int64) error {
	//Begin begins a transact
	tx := u.mysqlDb.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	//if tx.Error != nil {
	//	return tx.Error
	//}
	//删除Pod信息
	err := u.mysqlDb.Where("id=?", podID).Delete(&model.Pod{}).Error
	if err != nil {
		tx.Rollback()
		return err
	}
	//彻底删除podenv信息
	err = u.mysqlDb.Where("pod_id", podID).Delete(&model.PodEnv{}).Error
	if err != nil {
		tx.Rollback()
		return err
	}
	//彻底删除podport信息
	err = u.mysqlDb.Where("pod_id", podID).Delete(&model.PodPort{}).Error
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error

}

func (u *PodRepository) UpdatePod(pod *model.Pod) error {
	return u.mysqlDb.Model(pod).Update(pod).Error
}

// 获取结果集合
func (u *PodRepository) FindAll() (podAll []model.Pod, err error) {
	return podAll, u.mysqlDb.Find(&podAll).Error
}
