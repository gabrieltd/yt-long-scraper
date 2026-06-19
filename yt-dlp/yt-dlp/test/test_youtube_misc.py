#!/usr/bin/env python3

# Allow direct execution
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from test.helper import FakeYDL
from yt_dlp.extractor import YoutubeIE, YoutubeTabIE


class TestYoutubeMisc(unittest.TestCase):
    def test_youtube_extract(self):
        assertExtractId = lambda url, video_id: self.assertEqual(YoutubeIE.extract_id(url), video_id)
        assertExtractId('http://www.youtube.com/watch?&v=BaW_jenozKc', 'BaW_jenozKc')
        assertExtractId('https://www.youtube.com/watch?&v=BaW_jenozKc', 'BaW_jenozKc')
        assertExtractId('https://www.youtube.com/watch?feature=player_embedded&v=BaW_jenozKc', 'BaW_jenozKc')
        assertExtractId('https://www.youtube.com/watch_popup?v=BaW_jenozKc', 'BaW_jenozKc')
        assertExtractId('http://www.youtube.com/watch?v=BaW_jenozKcsharePLED17F32AD9753930', 'BaW_jenozKc')
        assertExtractId('BaW_jenozKc', 'BaW_jenozKc')

    def test_youtube_tab_approximate_upload_date(self):
        ie = YoutubeTabIE(FakeYDL({
            'extractor_args': {'youtubetab': {'approximate_date': ['']}},
        }))

        video = ie._extract_video({
            'videoId': 'BaW_jenozKc',
            'title': {'simpleText': 'Test video'},
            'publishedTimeText': {'simpleText': 'Jan 2, 2024'},
        })
        self.assertEqual(video['timestamp'], 1704153600)
        self.assertEqual(video['upload_date'], '20240102')

        lockup_video = ie._extract_lockup_view_model({
            'contentId': 'BaW_jenozKc',
            'contentType': 'LOCKUP_CONTENT_TYPE_VIDEO',
            'metadata': {
                'lockupMetadataViewModel': {
                    'title': {'content': 'Test video'},
                    'metadata': {
                        'contentMetadataViewModel': {
                            'metadataRows': [{
                                'metadataParts': [{
                                    'text': {'content': 'Jan 2, 2024'},
                                }],
                            }],
                        },
                    },
                },
            },
        })
        self.assertEqual(lockup_video['timestamp'], 1704153600)
        self.assertEqual(lockup_video['upload_date'], '20240102')

    def test_youtube_lockup_view_model_metadata(self):
        ie = YoutubeTabIE(FakeYDL())

        def lockup(metadata_rows, video_id='BaW_jenozKc'):
            return ie._extract_lockup_view_model({
                'contentId': video_id,
                'contentType': 'LOCKUP_CONTENT_TYPE_VIDEO',
                'metadata': {
                    'lockupMetadataViewModel': {
                        'title': {'content': 'Test video'},
                        'metadata': {
                            'contentMetadataViewModel': {
                                'metadataRows': metadata_rows,
                            },
                        },
                    },
                },
            })

        video = lockup([{
            'metadataParts': [{
                'text': {'content': '623K'},
                'leadingIcon': {'name': 'PLAY_ARROW_OUTLINED'},
            }, {
                'text': {'content': '1d ago'},
            }],
        }, {
            'badges': [{
                'badgeViewModel': {
                    'badgeText': 'Members only',
                    'badgeStyle': 'BADGE_MEMBERS_ONLY',
                },
            }],
        }])
        self.assertEqual(video['availability'], 'subscriber_only')
        self.assertEqual(video['view_count'], 623000)
        self.assertIsNone(video['live_status'])

        live_video = lockup([{
            'metadataParts': [{
                'text': {'content': '106 watching'},
            }],
        }])
        self.assertEqual(live_video['live_status'], 'is_live')
        self.assertEqual(live_video['concurrent_view_count'], 106)

        upcoming_video = lockup([{
            'metadataParts': [{
                'text': {'content': '5 waiting'},
            }, {
                'text': {'content': 'Scheduled for 6/16/26, 3:45 PM'},
            }],
        }])
        self.assertEqual(upcoming_video['live_status'], 'is_upcoming')
        self.assertEqual(upcoming_video['concurrent_view_count'], 5)

        past_live_video = lockup([{
            'metadataParts': [{
                'text': {'content': '374K views'},
            }, {
                'text': {'content': 'Streamed 4 days ago'},
            }],
        }])
        self.assertEqual(past_live_video['live_status'], 'was_live')
        self.assertEqual(past_live_video['view_count'], 374000)


if __name__ == '__main__':
    unittest.main()
