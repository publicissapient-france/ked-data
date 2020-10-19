# Comment exécuter moi-même le code pour récupérer les stats ?
1. Ouvrir dans intelliJ
2. Laisser maven travailler
3. exécuter en local dans intelliJ
4. récupérer le résultat dans le dossier racine **data_ked**, c'est du csv.

# Source de donnée
Les données viennent de [l'API PSE](https://api.xebia.fr/) mais j'ai pas réussi à m'y authentifier donc j'ai du demander à quelqu'un de m'envoyer le fichier.

On récupère un zip avec un dossier par année, un dossier par mois et un fichier json monoligne parfait pour spark. J'ai rapidement renommé les fichiers pour simuler un partitionning par année et par mois pour permettre à spark de rajouter ces colonnes très utiles dans le jeu de donnée.

5 colonnes + 2 que j'ai rajouté:
* year (rajouté)
* month (rajouté)
* startTime
* endTime
* attendees : liste de speaker, pas toujours bien renseigné
* summary
* description

# Contribution
Le code est bien dégueu donc libre à vous de contribuer dessus pour l'améliorer.

On pourrait créer une version notebook qui se déploierait automatiquement sur SageMaker dans votre compte AWS avec les données au bon endroit. Ça serait cool pour travailler dessus rapidos.
 