// Import des noms depuis les dependances
// Les depndances sont definies dans Cargo.toml
use polars::io::prelude::*;
use polars::lazy::prelude::*;
use polars::prelude::*;
use clap::{App, Arg};
use std::fs::File;
use std::path::Path;

// Simple fonction qui lit un CSV et retourne une Dataframe
fn example(path: &str) -> Result<DataFrame> {
    // always prefer `from_path` as that is fastest.
    CsvReader::from_path(path)?
        .infer_schema(None)
        .has_header(true)
        .finish()
} 

// Point d'entree, equivalent a main en C.
// Essai changement nom executif
fn main() {
    // Parametrage des options sur la ligne de commande
    let matches = App::new("Result parsing program")
        // Informations Generales
        .version("1.0")
        .author("Michèle Coulmance")
        .about("parses photo contests results")
        // Definition d'un argument
        .arg(
            // Argument nomme Path
            Arg::new("path")
                // Version courte (-p)
                .short('p')
                // Version longue --path=
                .long("path")
                // Necessaire
                .required(true)
                // Recoit une valeur (n'est pas un flag)
                .takes_value(true),
        )
        .arg(
            Arg::new("users")
                .short('u')
                .long("users")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    // Si on a un path, on le prend avec le nom p
    if let Some(p) = matches.value_of("path") {
        let zut: &str = &p.replace(".csv", "_result.csv");
        println!("{}",zut);
    // Si on a un path pour les users, on le prend avec le nom u
        if let Some(u) = matches.value_of("users") {
            // On cree une dataframe avec le fichier csv p
            let preres =
                example(p).unwrap();
            let nbvotreel: f64 = preres.height() as f64;
            // On cree une dataframe avec le fichier csv u
            let users = example(u).unwrap();
            // On recupere le nombre de votants possibles
            let nbvotpot: f64 = users.height() as f64;
            let rdtrois: f64 = nbvotreel / nbvotpot;
            println!("{:?}", rdtrois);
            // Get the column names
            let mut names = preres.get_column_names();
            // Remove the first two because they are not votes
            let votes: Vec<_> = names.drain(2..).collect();

            // On cree une nouvelle dataframe en faisant un leftjoin
            // equivalent a ce qu'on aurait en sql.
            let mut res = preres
                // On clone la valeur
                .clone()
                // On le convertit en lazy dataframe
                .lazy()
                // On fait la jointure
                .left_join(
                    users.lazy(),
                    col("Nom d'utilisateur"),
                    col("Nom d'utilisateur"),
                )
                // On reconvertit en eager dataframe
                .collect()
                // On a un result donc on unwrap
                .unwrap();

            let num_choices = votes.len();
            // C'est la qu'on va stocker le resultat de la suite
            let mut maybe_frame: Option<LazyFrame> = None;
            // Boucle de traitement principale
            for (i, nom) in votes.iter().enumerate() {
                // Remove self votes
                let mask = &res.column(nom).unwrap().eq(res.column("Prenom").unwrap());
                // On ignore le resultat de may_apply parceque la modif est appliquee sur place
                let _ignore = res.may_apply(nom, |s| s.utf8()?.set(&mask, Some("invalid")));
                if i == 0 {
                    // populate the first frame with the first vote count
                    let mut df = res
                        // Clone
                        .clone()
                        // Met en lazy
                        .lazy()
                        // Group par nom comme en sql
                        .groupby(vec![col(nom).alias("nom")])
                        // aggrege par decompte comme en sql
                        .agg(vec![col(nom).count().alias(nom)])
                        // Tri par nom
                        .sort_by_exprs(vec![col("nom")], vec![false])
                        // convertit en eager
                        .collect()
                        // Sort du Result
                        .unwrap();
                // On ignore le resultat de may_apply parceque la modif est appliquee sur place
                    let _res = df.may_apply(nom, |v| Ok(v * (num_choices - i )));

                    // On sauve la frame
                    maybe_frame = Some(df.lazy());
                } else {
                    let mut right = res
                        .clone()
                        .lazy()
                        .groupby(vec![col(nom).alias("nom")])
                        .agg(vec![col(nom).count().alias(nom)])
                        .sort_by_exprs(vec![col("nom")], vec![false])
                        .collect()
                        .unwrap();
                    let _res = right.may_apply(nom, |v| Ok(v * (num_choices - i )));

                    // on map sur une option
                    maybe_frame = maybe_frame.map(|frame| {
                        // On fait un outer join pour avoir les nouveaux resultats
                        frame.outer_join(right.lazy(), col("nom"), col("nom"))
                            .collect()
                            .unwrap()
                            .lazy()
                    });
                }
            }

            match maybe_frame {
                Some(frame) => {
                    // Si on a une frame
                    let mut frame = frame 
                        // on remplace Null par 0 sinon l'arithmetique
                        .fill_null(lit(0))
                        // on cree une nouvelle colonne total
                        .with_column(
                            sum_exprs(votes.iter().map(|v| col(v)).collect()).alias("total"),
                        )
                        .with_column(
                            (col("total").cast(DataType::Float64) * lit(rdtrois)).alias("average")
                        )
                        .sort_by_exprs(vec![col("total")], vec![true])
                        .collect();
                    let noms = &frame.as_ref().unwrap()["nom"].clone();
                    //println!("{:?}", &frame);
                    let _thenames = &frame.as_mut().unwrap().drop_in_place("nom");
                    //println!("{:?}", &frame);
                    let _toto = frame.as_mut().unwrap().insert_at_idx(0, noms.clone());
                    //println!("{:?}", &frame)

                    let la_sortie = Path::new(zut);
                    //let mut file = File::create("result.csv").unwrap();
                    let mut file = File::create(la_sortie).unwrap();
                    let _bob = CsvWriter::new(&mut file)
                    .has_headers(true)
                    .with_delimiter(b',')
                    .finish(frame.as_ref().unwrap());

                        // on montre le resultat
                    println!("{:?}", &frame);
                }
                None => {
                    println!("Something went terribly wrong");
                }
            }
        }
    }
}