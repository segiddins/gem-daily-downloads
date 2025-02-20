use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use anyhow::Context;
use chrono::NaiveDate;
use chrono::Utc;
use clap_derive::Subcommand;
use indicatif::ParallelProgressIterator;
use indicatif::ProgressBar;
use indicatif::ProgressIterator;
use indicatif::ProgressStyle;
use itertools::Itertools;
use rayon::iter::IntoParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use rusqlite::named_params;
use serde::Deserialize;
use serde::Serialize;
use url::Url;

use clap::Parser;
use clap_derive::Parser;

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn parse_date(arg: &str) -> chrono::ParseResult<chrono::NaiveDate> {
    chrono::NaiveDate::parse_from_str(arg, "%Y-%m-%d")
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Downloads the gem download data from bestgems.org
    Download {
        #[clap(long)]
        sqlite: Option<PathBuf>,
    },

    /// Query downloads for a single gem
    Gem {
        /// The name of the gem
        name: String,
        /// Start date
        #[clap(long, value_parser=parse_date)]
        start_date: Option<NaiveDate>,
        /// End date
        #[clap(long, value_parser=parse_date)]
        end_date: Option<NaiveDate>,
    },

    /// Query top gems by downloads over a period
    Top {
        /// Count of top gems to show
        #[clap(short = 'n', default_value = "10")]
        count: NonZeroUsize,
        /// Period to query
        #[clap(long, default_value = "28d")]
        duration: humantime::Duration,
        /// End date (defaults to today)
        #[clap(long, value_parser=parse_date)]
        end_date: Option<NaiveDate>,
        /// Only show gems that are new in the period
        #[clap(long)]
        only_new: bool,
    },
}

#[derive(Debug, Deserialize)]
struct BetterGem {
    date: NaiveDate,
    total_downloads: i64,
}

#[derive(Debug, Serialize)]
struct Download {
    date: NaiveDate,
    total_downloads: i64,
    daily_downloads: Option<i64>,
}

#[derive(Debug, Serialize)]
struct GemDownload<'a> {
    name: &'a str,
    total_downloads: i64,
    daily_downloads: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct DeGemDownload {
    name: String,
    total_downloads: i64,
}

fn parse_better_gem_file(file_path: &str) -> anyhow::Result<Vec<BetterGem>> {
    let file = std::fs::read_to_string(file_path)?;
    let mut better_gems: Vec<BetterGem> = serde_json::from_str(&file)?;
    better_gems.sort_by_key(|bg| bg.date);
    Ok(better_gems)
}

struct Downloads {
    name: String,
    downloads: Vec<Download>,
}

fn better_gems_to_downloads(name: &str, better_gems: Vec<BetterGem>) -> Downloads {
    if better_gems.is_empty() {
        return Downloads {
            name: name.to_owned(),
            downloads: vec![],
        };
    }

    let start_date = better_gems.first().unwrap().date.pred_opt().unwrap();
    let end_date = better_gems.last().unwrap().date;
    let size = (end_date - start_date).num_days() as usize;
    let mut dl = Downloads {
        name: name.to_string(),
        downloads: Vec::with_capacity(size),
    };

    let mut last_download: Download = Download {
        date: start_date,
        total_downloads: 0,
        daily_downloads: None,
    };
    for better_gem in better_gems {
        let days_between = (better_gem.date - last_download.date).num_days();
        // println!(
        //     "{}: {} days between {:?} and {:?}",
        //     name, days_between, last_download.date, better_gem.date
        // );
        if days_between == 1 {
            last_download.daily_downloads =
                Some(better_gem.total_downloads - last_download.total_downloads);
            dl.downloads.push(last_download);
            last_download = Download {
                date: better_gem.date,
                total_downloads: better_gem.total_downloads,
                daily_downloads: None,
            };
        } else {
            for i in 1..=days_between {
                let interpolated_date = last_download.date.succ_opt().unwrap();
                // println!("{}: {}", name, interpolated_date);
                let total_diff = better_gem.total_downloads - last_download.total_downloads;
                let total_downloads =
                    last_download.total_downloads + (total_diff * i / days_between);
                last_download.daily_downloads =
                    Some(total_downloads - last_download.total_downloads);
                let interpolated = Download {
                    date: interpolated_date,
                    total_downloads,
                    daily_downloads: None,
                };
                dl.downloads.push(last_download);
                last_download = interpolated;
            }
        }
    }
    dl.downloads.push(last_download);

    dl
}

fn group_downloads<'a>(
    downloads: &'a [Downloads],
    progress_style: &ProgressStyle,
) -> Vec<(NaiveDate, Vec<GemDownload<'a>>)> {
    let mut grouped: HashMap<NaiveDate, Vec<GemDownload<'a>>> = HashMap::new();
    downloads
        .iter()
        .progress()
        .with_prefix("Grouping by date")
        .with_style(progress_style.clone())
        .with_finish(indicatif::ProgressFinish::AndLeave)
        .for_each(|dl| {
            for download in &dl.downloads {
                grouped.entry(download.date).or_default().push(GemDownload {
                    name: &dl.name,
                    total_downloads: download.total_downloads,
                    daily_downloads: download.daily_downloads,
                });
            }
        });
    let mut dates: Vec<_> = grouped.into_iter().collect();
    dates.sort_by_key(|(date, _)| *date);
    dates
}

fn download_better_gems(progress_style: ProgressStyle) -> anyhow::Result<Vec<String>> {
    let names_url = "https://rubygems.org/names";
    let client = reqwest::blocking::Client::new();

    // Add names from most recent CSV, so we don't lose data for yanked gems
    let names = {
        let names: HashSet<String> = client
            .get(names_url)
            .send()?
            .text()?
            .split('\n')
            .map(str::to_string)
            .collect();

        let mut seen_names = std::fs::read_dir(std::path::Path::new("dates"))?
            .collect_vec()
            .into_iter()
            .rev()
            .take(100)
            .collect_vec()
            .into_par_iter()
            .progress_with_style(progress_style.clone())
            .with_message("Finding all seen gem names")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("csv"))
            .flat_map(|entry| {
                let mut rdr =
                    csv::Reader::from_path(entry.path()).expect("Could not read CSV file");
                rdr.deserialize::<DeGemDownload>()
                    .filter_map_ok(|record| Some(record.name))
                    .collect::<Result<HashSet<_>, _>>()
            })
            .reduce(
                || HashSet::<String>::new(),
                |mut acc, names| {
                    acc.extend(names);
                    acc
                },
            );

        let err_names = csv::Reader::from_path("errors.csv")
            .unwrap_or_else(|_| panic!("Failed to open errors.csv"))
            .deserialize::<(String, String)>()
            .filter_map_ok(|(name, _)| Some(name))
            .collect::<Result<HashSet<_>, _>>()
            .unwrap();

        seen_names.extend(err_names);
        seen_names.extend(names);
        seen_names.into_iter().sorted().collect_vec()
    };

    let progress_bar = ProgressBar::new(names.len() as u64)
        .with_style(progress_style)
        .with_prefix("Downloading better gems")
        .with_finish(indicatif::ProgressFinish::AndLeave);

    let today = Utc::now().naive_utc().date();

    let errors = std::sync::Mutex::new(Vec::<String>::new());

    let downloaded = names
        .par_iter()
        .progress_with(progress_bar)
        .filter(|name| {
            let path = better_gem_path(name);
            if serde_json::from_str::<Vec<BetterGem>>(
                &std::fs::read_to_string(&path).unwrap_or("[]".to_owned()),
            )
            .is_ok_and(|d| {
                d.first()
                    .unwrap_or(&BetterGem {
                        date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
                        total_downloads: 0,
                    })
                    .date
                    == today
            }) {
                return true;
            }

            download_better_gem(&client, name)
                .inspect_err(|e| {
                    eprintln!("error downloading {:?}: {}", name, e);
                    errors
                        .lock()
                        .unwrap()
                        .push(format!("\"{}\",\"{}\"\n", name, e));
                })
                .is_ok()
        })
        .cloned()
        .collect::<Vec<_>>();

    let mut errors = errors.lock().unwrap();
    errors.sort();
    std::fs::write("errors.csv", errors.join(""))?;

    Ok(downloaded)
}

fn download_better_gem(client: &reqwest::blocking::Client, name: &str) -> anyhow::Result<()> {
    let mut url = Url::parse("https://bestgems.org/api/v1/gems")?;
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("Failed to parse URL"))?
        .extend([name, "total_downloads.json"]);

    let mut response = client.get(url.as_str()).send()?.error_for_status()?;

    let path = better_gem_path(name);
    let mut file = std::fs::File::create(path)?;
    response.copy_to(&mut file)?;
    Ok(())
}

fn better_gem_path(name: &str) -> String {
    format!(
        "bettergems/{}.json",
        name.bytes()
            .map(|c| {
                if c.is_ascii_lowercase() || c == b'-' || c == b'_' || c.is_ascii_digit() {
                    String::from_utf8(vec![c]).unwrap()
                } else {
                    format!("%{:02X}", c)
                }
            })
            .collect::<String>()
    )
}

#[test]
fn test_better_gem_path() {
    assert_eq!(better_gem_path("aa"), "bettergems/aa.json");
    assert_eq!(better_gem_path("a a"), "bettergems/a%20a.json");
    assert_eq!(better_gem_path("foo-bar"), "bettergems/foo-bar.json");
    assert_eq!(better_gem_path("foo_bar"), "bettergems/foo_bar.json");
    assert_eq!(
        better_gem_path("foo-bar_baz"),
        "bettergems/foo-bar_baz.json"
    );
    assert_eq!(
        better_gem_path("foo-bar_baz-qux"),
        "bettergems/foo-bar_baz-qux.json"
    );
    assert_eq!(
        better_gem_path("foo-bar_baz-qux-123"),
        "bettergems/foo-bar_baz-qux-123.json"
    );
    assert_eq!(better_gem_path("AAaa"), "bettergems/%41%41aa.json");
}

fn download(sqlite_path: Option<PathBuf>) {
    fs::create_dir_all("bettergems").unwrap();
    fs::create_dir_all("dates").unwrap();

    let progress_style = ProgressStyle::default_bar()
        .template("{prefix} {msg} {elapsed_precise} {percent}% {per_sec} ETA {eta} {wide_bar:.green} {pos}/{len}")
        .unwrap();

    let names = download_better_gems(progress_style.clone()).unwrap();

    let downloads = names
        .par_iter()
        .progress()
        .with_prefix("Parsing json files")
        .with_style(progress_style.clone())
        .with_finish(indicatif::ProgressFinish::AndLeave)
        .map(|name| {
            let path = better_gem_path(name);
            let better_gems = parse_better_gem_file(&path).unwrap_or_else(|e| {
                println!("Error parsing {}: {}", path, e);
                vec![]
            });
            better_gems_to_downloads(name, better_gems)
        })
        .collect::<Vec<_>>();

    let dates = group_downloads(&downloads, &progress_style);
    dates
        .par_iter()
        .progress()
        .with_prefix("Writing csv files")
        .with_style(progress_style.clone())
        .with_finish(indicatif::ProgressFinish::AndLeave)
        .for_each(|(date, downloads)| {
            let f = std::fs::File::create(date_path(*date))
                .unwrap_or_else(|e| panic!("Failed to create file {}: {}", date_path(*date), e));
            let mut wtr = csv::Writer::from_writer(f);
            for download in downloads {
                wtr.serialize(download).unwrap();
            }
            wtr.flush().unwrap();
        });

    if let Some(sqlite_path) = sqlite_path {
        let mut conn = rusqlite::Connection::open(sqlite_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = OFF;
              PRAGMA synchronous = 0;
              PRAGMA cache_size = 1000000;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;",
        )
        .expect("PRAGMA");
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS downloads (
                date DATE,
                gem_name TEXT,
                total bigint,
                daily bigint,
                PRIMARY KEY (date, gem_name)
            );
        "#,
            [],
        )
        .unwrap();

        {
            {
                dates
                .iter()
                .progress()
                .with_prefix("Inserting into sqlite")
                .with_style(progress_style)
                .with_finish(indicatif::ProgressFinish::AndLeave)
                .for_each(|(date, downloads)| {
                    {
                        let mut tx = conn.transaction().unwrap();
                        tx.set_drop_behavior(rusqlite::DropBehavior::Commit);
                        {
                            let mut stmt = tx.prepare_cached(r#"
                INSERT OR REPLACE INTO downloads (date, gem_name, total, daily) VALUES (:date, :gem_name, :total, :daily);
            "#).unwrap();

                            for download in downloads {
                                stmt.execute(named_params! {
                                    ":date": date.to_string(),
                                    ":gem_name": download.name,
                                    ":total": download.total_downloads,
                                    ":daily": download.daily_downloads,
                                })
                                .unwrap();
                            }
                        }
                        tx.commit().unwrap();
                }
                });
            }
        }

        conn.execute("VACUUM;", []).unwrap();
    }
}

fn date_path(date: NaiveDate) -> String {
    use chrono::Datelike;
    format!("dates/{}/{:02}/{}.csv", date.year(), date.month(), date)
}

fn read_gem_downloads(date: NaiveDate) -> anyhow::Result<HashMap<String, DeGemDownload>> {
    let path = date_path(date);
    let file = fs::File::open(&path).with_context(|| format!("Failed to open file {}", path))?;
    let mut rdr = csv::Reader::from_reader(file);

    let deserialized = rdr
        .deserialize::<DeGemDownload>()
        .fold_ok(
            HashMap::new(),
            |mut hm: HashMap<String, DeGemDownload>, gd| {
                hm.insert(gd.name.to_string(), gd);
                hm
            },
        )
        .with_context(|| format!("Failed to read csv file {}", path))?;

    Ok(deserialized)
}

fn top(
    count: NonZeroUsize,
    duration: humantime::Duration,
    end: NaiveDate,
    only_new: bool,
) -> anyhow::Result<()> {
    let start = end - chrono::Duration::from_std(*duration).unwrap();

    let start_downloads = read_gem_downloads(start)?;
    let end_downloads = read_gem_downloads(end)?;

    #[derive(Debug)]
    struct Diff<'a> {
        name: &'a str,
        diff: i64,
        start: i64,
        end: i64,
    }

    let top = end_downloads
        .values()
        .map(|end| {
            let start = start_downloads.get(&end.name);
            let start_downloads = start.map_or(0, |d| d.total_downloads);
            Diff {
                name: &end.name,
                start: start_downloads,
                end: end.total_downloads,
                diff: end.total_downloads - start_downloads,
            }
        })
        .sorted_by_key(|gd| -gd.diff)
        .filter(|gd| !only_new || gd.start == 0)
        .take(count.get())
        .collect_vec();

    println!("Top {} gems by downloads from {} to {}", count, start, end);

    println!(
        "{: <60} {: >10} {: >10} {: >10}",
        "Name", "Start", "End", "Diff"
    );
    println!("{:-<60} {:-<10} {:-<10} {:-<10}", "", "", "", "");
    for gd in top {
        println!(
            "{: <60} {: >10} {: >10} {: >10}",
            gd.name, gd.start, gd.end, gd.diff
        );
    }
    Ok(())
}

fn main() {
    let command = Cli::parse();

    match command.command {
        Commands::Download { sqlite } => download(sqlite),
        Commands::Top {
            count,
            duration,
            end_date,
            only_new,
        } => top(
            count,
            duration,
            end_date.unwrap_or_else(|| Utc::now().naive_utc().date()),
            only_new,
        )
        .unwrap(),
        _ => unreachable!(),
    }
}
