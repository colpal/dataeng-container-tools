use bollard::Docker;
use bollard::models::ContainerCreateBody;
use bollard::query_parameters::{CreateContainerOptions, RemoveContainerOptionsBuilder, AttachContainerOptionsBuilder, RemoveImageOptionsBuilder, StartContainerOptions};
use futures_util::stream::StreamExt;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use tokio::task::spawn;

#[cfg(not(windows))]
use std::io::{stdout, Read, Write};
#[cfg(not(windows))]
use termion::async_stdin;
#[cfg(not(windows))]
use termion::raw::IntoRawMode;
#[cfg(not(windows))]
use tokio::time::sleep;
#[cfg(not(windows))]
use std::time::Duration;

use super::{build_image, get_image_tag};

pub async fn test_container(path: &Path, bash: bool, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    let docker = Docker::connect_with_local_defaults()?;
    
    // Build the image first
    build_image(path, verbose).await?;
    
    // Get the image tag that was built
    let image_tag = get_image_tag(path);
    
    println!("\nStarting container for testing...");
    
    // Configure container
    let mut config = ContainerCreateBody {
        image: Some(image_tag.clone()),
        tty: Some(true),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        open_stdin: Some(true),
        attach_stdin: Some(true),
        ..Default::default()
    };
    
    // Override with bash if requested
    if bash {
        config.entrypoint = Some(vec!["/bin/bash".to_string()]);
        config.cmd = Some(vec![]);
        println!("Interactive bash mode enabled");
    }
    
    let container = docker.create_container(None::<CreateContainerOptions>, config).await?;
    let container_id = container.id.clone();
    
    println!("✓ Container created: {}", container_id);
    
    // Cleanup function
    let cleanup = || async {
        println!("\nCleaning up...");
        
        // Remove container
        let remove_options = RemoveContainerOptionsBuilder::default()
            .force(true)
            .build();
        if let Err(e) = docker.remove_container(&container_id, Some(remove_options)).await {
            eprintln!("Warning: Failed to remove container: {}", e);
        } else {
            println!("✓ Container removed");
        }
        
        // Remove image
        let remove_image_options = RemoveImageOptionsBuilder::default()
            .force(true)
            .build();
        if let Err(e) = docker.remove_image(&image_tag, Some(remove_image_options), None).await {
            eprintln!("Warning: Failed to remove image: {}", e);
        } else {
            println!("✓ Image removed");
        }
    };
    
    // Start and attach to container
    docker.start_container(&container_id, None::<StartContainerOptions>).await?;
    println!("✓ Container started");
    
    // Interactive mode with proper TTY handling
    let attach_options = AttachContainerOptionsBuilder::default()
        .stdout(true)
        .stderr(true)
        .stdin(true)
        .stream(true)
        .build();
    
    let bollard::container::AttachContainerResults {
        mut output,
        mut input,
    } = docker.attach_container(&container_id, Some(attach_options)).await?;
    
    #[cfg(not(windows))]
    {
        // Pipe stdin into the docker attach stream input (Unix-like systems with termion)
        spawn(async move {
            #[allow(clippy::unbuffered_bytes)]
            let mut stdin = async_stdin().bytes();
            loop {
                if let Some(Ok(byte)) = stdin.next() {
                    input.write_all(&[byte]).await.ok();
                } else {
                    sleep(Duration::from_nanos(10)).await;
                }
            }
        });
        
        // Set stdout in raw mode for TTY
        let stdout = stdout();
        let mut stdout = stdout.lock().into_raw_mode()?;
        
        // Pipe docker attach output into stdout
        while let Some(Ok(output)) = output.next().await {
            stdout.write_all(output.into_bytes().as_ref())?;
            stdout.flush()?;
        }
    }
    
    #[cfg(windows)]
    {
        // Windows: use tokio stdin without raw mode (termion not available)
        use tokio::io::{AsyncReadExt, stdin};
        
        spawn(async move {
            let mut stdin = stdin();
            let mut buf = [0u8; 1];
            loop {
                if stdin.read_exact(&mut buf).await.is_ok() {
                    input.write_all(&buf).await.ok();
                }
            }
        });
        
        // Pipe docker attach output into stdout
        use std::io::{stdout, Write};
        let mut stdout = stdout();
        while let Some(Ok(output)) = output.next().await {
            stdout.write_all(output.into_bytes().as_ref())?;
            stdout.flush()?;
        }
    }
    
    // Cleanup
    cleanup().await;
    
    Ok(())
}
