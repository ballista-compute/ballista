//! Ballista cluster management utilities

use std::env;
use std::fs;
use std::fs::File;
use std::process::Command;
use std::str;

use crate::error::BallistaError;
use k8s_openapi;
use k8s_openapi::api::core::v1 as api;
use k8s_openapi::http;
use reqwest;
use std::io::Write;

#[derive(Gtmpl)]
struct ApplicationTemplateVariables {
    name: String,
}

#[derive(Gtmpl)]
struct ExecutorTemplateVariables {
    name: String,
}

fn execute(request: http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>, BallistaError> {
    let (method, path, body) = {
        let (parts, body) = request.into_parts();
        let mut url: http::uri::Parts = parts.uri.into();
        let path = url
            .path_and_query
            .take()
            .expect("request doesn't have path and query");

        (parts.method, path, body)
    };

    //TODO: this is hard-coded for local minikube
    let uri = format!("http://localhost:8001{}", path);

    let client = reqwest::Client::new();

    //    println!(
    //        "Request: {} {}{}",
    //        method,
    //        uri,
    //        str::from_utf8(&body).unwrap()
    //    );

    let mut x = match method {
        http::Method::GET => client.get(&uri).body(body).send()?,
        http::Method::POST => client.post(&uri).body(body).send()?,
        http::Method::DELETE => client.delete(&uri).body(body).send()?,
        _ => unimplemented!(),
    };

    let response_body = x.text()?;

    if x.status().is_success() {
        let response = http::Response::builder()
            .status(http::StatusCode::OK)
            .body(response_body.as_bytes().to_vec())?;

        Ok(response)
    } else {
        println!("Response: {}", response_body);

        Err(BallistaError::General("k8s api returned error".to_string()))
    }
}

/// Ballista executor
pub struct Executor {
    pub host: String,
    pub port: usize,
}

/// Get a list of executor nodes in a cluster using Kubernetes service discovery
pub fn get_executors(cluster_name: &str) -> Result<Vec<Executor>, BallistaError> {
    let mut executors: Vec<Executor> = vec![];
    let mut instance = 1;
    loop {
        let host_env = format!("BALLISTA_{}_{}_SERVICE_HOST", cluster_name, instance);
        let port_env = format!("BALLISTA_{}_{}_SERVICE_PORT_GRPC", cluster_name, instance);
        match (env::var(&host_env), env::var(&port_env)) {
            (Ok(host), Ok(port)) => executors.push(Executor {
                host,
                port: port.parse::<usize>().unwrap(),
            }),
            _ => break,
        }
        instance += 1;
    }
    Ok(executors)
}

/// Create a Ballista executor pod and service in Kubernetes
pub fn create_ballista_executor(
    _namespace: &str,
    name: &str,
    image_name: &str,
) -> Result<(), BallistaError> {
    let x = ExecutorTemplateVariables {
        name: name.to_string(),
    };

    let executor_template = fs::read_to_string(image_name)?;

    let executor_yaml = gtmpl::template(&executor_template, x)?;

    //println!("{}", executor_yaml);

    //TODO unique filename
    let mut f = File::create("temp.yaml")?;
    f.write_all(executor_yaml.as_bytes())?;

    // shell out to kubectl
    Command::new("kubectl")
        .arg("apply")
        .arg("-f")
        .arg("temp.yaml")
        .output()
        .expect("failed to execute process");

    Ok(())
}

/// Create a Ballista application pod in Kubernetes
pub fn create_ballista_application(
    _namespace: &str,
    name: &str,
    image_name: &str,
) -> Result<(), BallistaError> {
    let x = ApplicationTemplateVariables {
        name: name.to_string(),
    };

    let executor_template = fs::read_to_string(image_name)?;

    let executor_yaml = gtmpl::template(&executor_template, x)?;

    //println!("{}", executor_yaml);

    //TODO unique filename
    let mut f = File::create("temp.yaml")?;
    f.write_all(executor_yaml.as_bytes())?;

    // shell out to kubectl
    Command::new("kubectl")
        .arg("apply")
        .arg("-f")
        .arg("temp.yaml")
        .output()
        .expect("failed to execute process");

    Ok(())
}

/// Delete a Kubernetes pod
pub fn delete_pod(namespace: &str, pod_name: &str) -> Result<(), BallistaError> {
    let (request, response_body) =
        api::Pod::delete_namespaced_pod(pod_name, namespace, Default::default())
            .expect("couldn't delete pod");
    let response = execute(request).expect("couldn't delete pod");

    // Got a status code from executing the request.
    let status_code: http::StatusCode = response.status();

    let mut response_body = response_body(status_code);
    response_body.append_slice(&response.body());
    let response = response_body.parse();

    match response {
        // Successful response (HTTP 200 and parsed successfully)
        Ok(api::DeleteNamespacedPodResponse::OkStatus(_status)) => {
            //println!("deleted pod {} ok: status {:?}", pod_name, status);
            Ok(())
        }

        Ok(api::DeleteNamespacedPodResponse::OkValue(_value)) => {
            //println!("deleted pod {} ok: value {:?}", pod_name, value);
            Ok(())
        }

        Ok(api::DeleteNamespacedPodResponse::Accepted(_status)) => {
            //println!("deleted pod {} ok: accepted status {:?}", pod_name, status);
            Ok(())
        }

        // Some unexpected response
        // (not HTTP 200, but still parsed successfully)
        Ok(other) => Err(format!("expected Ok but got {} {:?}", status_code, other).into()),

        // Need more response data.
        // Read more bytes from the response into the `ResponseBody`
        Err(k8s_openapi::ResponseError::NeedMoreData) => Err(BallistaError::General(
            "Need more response data".to_string(),
        )),

        // Some other error, like the response body being
        // malformed JSON or invalid UTF-8.
        Err(err) => Err(format!("error: {} {:?}", status_code, err).into()),
    }
}

/// Delete a Kubernetes service
pub fn delete_service(namespace: &str, service_name: &str) -> Result<(), BallistaError> {
    let (request, response_body) =
        api::Service::delete_namespaced_service(service_name, namespace, Default::default())
            .expect("couldn't delete service");
    let response = execute(request).expect("couldn't delete service");

    // Got a status code from executing the request.
    let status_code: http::StatusCode = response.status();

    let mut response_body = response_body(status_code);
    response_body.append_slice(&response.body());
    let response = response_body.parse();

    match response {
        // Successful response (HTTP 200 and parsed successfully)
        Ok(api::DeleteNamespacedServiceResponse::OkStatus(_status)) => {
            //println!("deleted pod {} ok: status {:?}", pod_name, status);
            Ok(())
        }

        Ok(api::DeleteNamespacedServiceResponse::OkValue(_value)) => {
            //println!("deleted pod {} ok: value {:?}", pod_name, value);
            Ok(())
        }

        Ok(api::DeleteNamespacedServiceResponse::Accepted(_status)) => {
            //println!("deleted pod {} ok: accepted status {:?}", pod_name, status);
            Ok(())
        }

        // Some unexpected response
        // (not HTTP 200, but still parsed successfully)
        Ok(other) => Err(format!("expected Ok but got {} {:?}", status_code, other).into()),

        // Need more response data.
        // Read more bytes from the response into the `ResponseBody`
        Err(k8s_openapi::ResponseError::NeedMoreData) => Err(BallistaError::General(
            "Need more response data".to_string(),
        )),

        // Some other error, like the response body being
        // malformed JSON or invalid UTF-8.
        Err(err) => Err(format!("error: {} {:?}", status_code, err).into()),
    }
}

/// Get a list of Kubernetes pods
pub fn list_pods(namespace: &str) -> Result<Vec<String>, BallistaError> {
    let (request, response_body) =
        api::Pod::list_namespaced_pod(namespace, Default::default()).expect("couldn't list pods");
    let response = execute(request).expect("couldn't list pods");

    // Got a status code from executing the request.
    let status_code: http::StatusCode = response.status();

    // Construct the `ResponseBody<ListNamespacedPodResponse>` using the
    // constructor returned by the API function.
    let mut response_body = response_body(status_code);

    response_body.append_slice(&response.body());

    let response = response_body.parse();
    match response {
        // Successful response (HTTP 200 and parsed successfully)
        Ok(api::ListNamespacedPodResponse::Ok(pod_list)) => {
            let mut ret = vec![];
            for pod in pod_list.items {
                ret.push(pod.metadata.unwrap().name.unwrap());
            }
            Ok(ret)
        }

        // Some unexpected response
        // (not HTTP 200, but still parsed successfully)
        Ok(other) => Err(format!("expected Ok but got {} {:?}", status_code, other).into()),

        // Need more response data.
        // Read more bytes from the response into the `ResponseBody`
        Err(k8s_openapi::ResponseError::NeedMoreData) => Err(BallistaError::General(
            "Need more response data".to_string(),
        )),

        // Some other error, like the response body being
        // malformed JSON or invalid UTF-8.
        Err(err) => Err(format!("error: {} {:?}", status_code, err).into()),
    }
}
