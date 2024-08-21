package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	// "reflect"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	batch_defaults "k8s.io/kubernetes/pkg/apis/batch/v1"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/job/job"
)

func init() {
    // Set the log level to Fatal (0)
	klog.InitFlags(nil)
    flag.Set("v", "9")  // Only show fatal logs
}

func toJob(attr events.DynamoDBAttributeValue) *batch.Job {
	// klog.Info("In toJob(), with attr: %v", attr)
	job := &batch.Job{}
	// klog.Infof("Type of Job is %s", reflect.TypeOf(job))
	err := json.Unmarshal([]byte(attr.String()), job)
	if err != nil {
		klog.Errorf("error unmarshalling job: %v", err)
		return nil
	}

	return job
}

func toPod(attr events.DynamoDBAttributeValue) *v1.Pod {
	// klog.Info("In toPod(), with attr: %s", attr.String())
	pod := &v1.Pod{}

	err := json.Unmarshal([]byte(attr.String()), pod)
	if err != nil {
		klog.Errorf("error unmarshalling pod: %v", err)
		return nil
	}

	return pod
}

func JobHandler(ctx context.Context, event events.DynamoDBEvent) error {
	// klog.Infof("Starting job controller")
	// defer klog.Infof("Shutting down job controller")

	// klog.Infof("Type of event is %s", reflect.TypeOf(event))

	for _, record := range event.Records {
		// klog.Infof("In Job Handler, %v", record)
		if record.EventName == "MODIFY" || record.EventName == "REMOVE" {
			return nil	
		}

		jm := job.NewController()
		isJob := false
		var err error

		if record.Change.NewImage["resource-type"].String() == "job" || record.Change.OldImage["resource-type"].String() == "job" {
			isJob = true
		}

		switch record.EventName {
		case "INSERT":
			// klog.Info("Handling INSERT")
			err = handleInsert(record.Change.NewImage, isJob, jm)
		case "MODIFY":
			// klog.Info("Handling MODIFY")
			err = handleModify(record.Change.NewImage, record.Change.OldImage, isJob, jm)
			// return nil
		case "REMOVE":
			// klog.Info("Handling REMOVE")
			err = handleRemove(record.Change.OldImage, isJob, jm)
			// return nil
		}

		if err != nil {
			klog.Infof("Error: %v", err)
			return nil
		}

		// klog.Infof("About to process next work item")
		jm.ProcessNextWorkItem()

		// klog.Infof("About to process next orphan pod")
		jm.ProcessNextOrphanPod()
	}

	return nil
}

func handleInsert(curr map[string]events.DynamoDBAttributeValue, isJob bool, jm *job.Controller) error {
	if isJob {
		job := toJob(curr["resourceValue"])
		batch_defaults.SetDefaults_Job(job)
		controller.UpdateJob(job)

		if job != nil {
			jm.EnqueueController(job, true)
			return nil
		}
	} else {
		pod := toPod(curr["resourceValue"])

		if pod != nil {
			jm.AddPod(pod)
			return nil
		}
	}

	return fmt.Errorf("error while handling INSERT")
}

func handleModify(curr map[string]events.DynamoDBAttributeValue, old map[string]events.DynamoDBAttributeValue, isJob bool, jm *job.Controller) error {
	if isJob {
		oldJob := toJob(old["resourceValue"])
		currJob := toJob(curr["resourceValue"])

		if oldJob != nil && currJob != nil {
			jm.UpdateJob(oldJob, currJob)
			return nil
		}
	} else {
		oldPod := toPod(old["resourceValue"])
		currPod := toPod(curr["resourceValue"])

		if oldPod != nil && currPod != nil {
			jm.UpdatePod(oldPod, currPod)
			return nil
		}
	}

	return fmt.Errorf("error while handling MODIFY")
}

func handleRemove(old map[string]events.DynamoDBAttributeValue, isJob bool, jm *job.Controller) error {
	if isJob {
		job := toJob(old["resourceValue"])

		if job != nil {
			jm.DeleteJob(job)
			return nil
		}
	} else {
		pod := toPod(old["resourceValue"])

		if pod != nil {
			jm.DeletePod(pod, true)
			return nil
		}
	}

	return fmt.Errorf("error while handling REMOVE")
}

func main() {
	lambda.Start(JobHandler)
}
