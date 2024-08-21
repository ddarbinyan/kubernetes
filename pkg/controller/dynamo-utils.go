package controller

import (
	"fmt"
	"time"

	batch "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/klog/v2"
	v1_defaults "k8s.io/kubernetes/pkg/apis/core/v1"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	maxNameLength          = 63
	randomLength           = 5
	MaxGeneratedNameLength = maxNameLength - randomLength
)

var (
	sess  = session.Must(session.NewSession(&aws.Config{
        Region:      aws.String("eu-north-1"),
        Credentials: credentials.NewEnvCredentials(), // Explicitly use environment variable credentials
    }))
	svc   = dynamodb.New(sess)
	table = "kube-db"
)

func listAll(resourceType string) ([]map[string]*dynamodb.AttributeValue, error) {
	input := &dynamodb.ScanInput{
		TableName:        aws.String(table),
		FilterExpression: aws.String("#rt = :resourceType"),
		ProjectionExpression: aws.String("resourceValue"),
		ExpressionAttributeNames: map[string]*string{
			"#rt": aws.String("resource-type"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":resourceType": {
				S: aws.String(resourceType),
			},
		},
	}

	result, err := svc.Scan(input)
	
	if err != nil {
		return nil, fmt.Errorf("failed to scan DynamoDB table: %v", err)
	}

	return result.Items, nil
}

func ListJobs() ([]*batch.Job, error){
	// klog.Infof("Trying to list jobs from Dynamo DB")

	var list []*batch.Job
	items, err := listAll("job")
	
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		job := &batch.Job{}
		err = json.Unmarshal([]byte(*item["resourceValue"].S), job)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal DynamoDB item to Job: %v", err)
		}
		// klog.V(4).Infof("Job Appended to list: %v", job)

		list = append(list, job)
	}

	return list, nil
}

func ListPods(selector labels.Selector) ([]*v1.Pod, error){
	// klog.Infof("Trying to list pods from Dynamo DB")

	var list []*v1.Pod
	items, err := listAll("pod")
	
	if err != nil {
		return nil, err
	}

	selectAll := selector.Empty()
	for _, item := range items {
		pod := &v1.Pod{}
		err = json.Unmarshal([]byte(*item["resourceValue"].S), pod)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal DynamoDB item to Pod: %v", err)
		}
		
		// klog.V(4).Infof("Pod Appended to list: %v", pod)

		if selectAll {
			list = append(list, pod)
			continue
		}

		metadata, err := meta.Accessor(pod)
		if err != nil {
			return nil, fmt.Errorf("error in meta.Accessor(pod): %v", err)
		}
		if selector.Matches(labels.Set(metadata.GetLabels())) {
			list = append(list, pod)
		}
	}

	return list, nil
}

func get(resourceType string, resourceName string, desiredValue string) (*dynamodb.AttributeValue, error) {
	input := &dynamodb.GetItemInput{
		TableName:        aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{"resource-type": {
                S: aws.String(resourceType),
            },
            "resource-name": {
                S: aws.String(resourceName),
            },
        },
		ProjectionExpression: aws.String(desiredValue),
	}

	result, err := svc.GetItem(input)
   
	if err != nil {
        return nil, fmt.Errorf("failed to get item from DynamoDB table: %v", err)
    }

	if result.Item == nil {
        return nil, fmt.Errorf("%s %s not found in DynamoDB table", resourceType, resourceName)
    }

	return result.Item[desiredValue], nil
}

func GetJob(resourceName string) (*batch.Job, error) {
	// klog.Infof("Trying to get job %s from Dynamo DB", resourceName)
	item, err := get("job", resourceName, "resourceValue")
	if err != nil {
		return nil, err
	}

	job := &batch.Job{}
	err = json.Unmarshal([]byte(*item.S), job)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal DynamoDB item to Job: %v", err)
	}

	// klog.Infof("Was able to get job %s from Dynamo DB", job.Name)

	return job, nil
}

func GetPod(resourceName string) (*v1.Pod, error) {
	// klog.Infof("Trying to get pod %s from Dynamo DB", resourceName)

	item, err := get("pod", resourceName, "resourceValue")
	if err != nil {
		return nil, err
	}

	pod := &v1.Pod{}
	err = json.Unmarshal([]byte(*item.S), pod)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal DynamoDB item to Pod: %v", err)
	}

	return pod, nil
}

func GetExpectations(resourceName string) (*ControlleeExpectations, error) {
	// klog.Infof("Trying to get expectations for job %s from Dynamo DB", resourceName)

	item, err := get("job", resourceName, "expectations")
	if err != nil {
		return nil, err
	}

	exp := &ControlleeExpectations{}
	err = json.Unmarshal([]byte(*item.S), exp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal DynamoDB item to Expectation: %v", err)
	}

	return exp, nil
}

// GetPodJobs returns a list of Jobs that potentially
// match a Pod. Only the one specified in the Pod's ControllerRef
// will actually manage it.
// Returns an error only if no matching Jobs are found.
func GetPodJobs(pod *v1.Pod) (jobs []batch.Job, err error) {
	// klog.Infof("Trying to get jobs mathcing pod %s from Dynamo DB", pod.Name)

	if len(pod.Labels) == 0 {
		err = fmt.Errorf("no jobs found for pod %v because it has no labels", pod.Name)
		return
	}

	var list []*batch.Job
	list, err = ListJobs()
	if err != nil {
		return
	}
	for _, job := range list {
		selector, err := metav1.LabelSelectorAsSelector(job.Spec.Selector)
		if err != nil {
			// This object has an invalid selector, it does not match the pod
			continue
		}
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		jobs = append(jobs, *job)
	}
	if len(jobs) == 0 {
		err = fmt.Errorf("could not find jobs for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}
	return
}

func DeletePod(podID string, object *batch.Job) error {
	// klog.Infof("Trying to not pod %s from Dynamo DB", podID)

	accessor, err := meta.Accessor(object)
	if err != nil {
		return fmt.Errorf("object does not have ObjectMeta, %v", err)
	}
	klog.V(2).InfoS("Deleting pod", "controller", accessor.GetName(), "pod", podID)

	key := map[string]*dynamodb.AttributeValue{
		"resource-type": {
			S: aws.String("pod"),
		},
		"resource-name": {
			S: aws.String(podID),
		},
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(table),
		Key:       key,
	}

	if _, err :=svc.DeleteItem(input); err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			klog.V(4).Infof("pod %v/%v has already been deleted.", podID)
			return err
		}
		klog.V(4).Infof("Error deleting: %v", err)
		return fmt.Errorf("unable to delete pods: %v", err)
	}
	// klog.V(4).Infof("Deleted pod: %v", podID)

	return nil
}

func UpdateJob(job *batch.Job) (error){
	// klog.Infof("Trying to update job %s from Dynamo DB\n Updated job is %v", job.Name, job)
	
    jobJSON, err := json.Marshal(job)
    if err != nil {
        return fmt.Errorf("failed to marshal job to JSON: %v", err)
    }

    key := map[string]*dynamodb.AttributeValue{
        "resource-type": {
			S: aws.String("job"),
        },
        "resource-name": {
			S: aws.String(job.Name),
        },
    }

    input := &dynamodb.UpdateItemInput{
        TableName: aws.String(table),
        Key:       key,
        UpdateExpression: aws.String("SET resourceValue = :rv"),
        ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
            ":rv": {
                S: aws.String(string(jobJSON)),
            },
        },
    }

    _, err = svc.UpdateItem(input)
    if err != nil {
        return fmt.Errorf("failed to update item in DynamoDB: %v", err)
    }

    return nil
}

func CreatePod(pod *v1.Pod, object runtime.Object) error {
    if len(labels.Set(pod.Labels)) == 0 {
        return fmt.Errorf("unable to create pods, no labels")
    }

	if len(pod.GenerateName) > MaxGeneratedNameLength {
		pod.GenerateName = pod.GenerateName[:MaxGeneratedNameLength]
	}
	pod.Name = fmt.Sprintf("%s%s", pod.GenerateName, utilrand.String(randomLength))
	v1_defaults.SetDefaults_Pod(pod)

    podJSON, err := json.Marshal(pod)
    if err != nil {
        return fmt.Errorf("failed to marshal pod to JSON: %v", err)
    }

	// klog.Infof("Trying to create pod %s into Dynamo DB", pod.Name)

    item := map[string]*dynamodb.AttributeValue{
        "resource-type": {
			S: aws.String("pod"),
        },
        "resource-name": {
			S: aws.String(pod.Name),
        },
        "resourceValue": {
            S: aws.String(string(podJSON)),
        },
		"creationTimestamp": {
			S: aws.String(fmt.Sprintf("%d", time.Now().UnixMilli())),
		},
    }

    // Put the item in DynamoDB
    input := &dynamodb.PutItemInput{
        TableName: aws.String(table),
        Item:      item,
    }

    _, err = svc.PutItem(input)
    if err != nil {
        return fmt.Errorf("failed to put item in DynamoDB: %v", err)
    }

    accessor, err := meta.Accessor(object)
	accessor.GetCreationTimestamp()
    if err != nil {
        klog.Errorf("parentObject does not have ObjectMeta, %v", err)
        return nil
    }
    klog.V(4).Infof("Controller %v created pod %v", accessor.GetName(), pod.Name)

    return nil
}
